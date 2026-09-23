# Model and LoRA cleanup - 2026-09-23

Owner: "почисти старые неиспользуемые модели и лоры". Retired files are **moved, not
deleted**, into a quarantine folder on the same drive of the same box:
`<drive>:\_retired_20260923\<original path without the drive letter>`. The owner purges
the quarantine folders when satisfied (commands at the end). Every move is also logged on
the box in `C:\ProgramData\AutoRig\model-cleanup-20260923.log`.

## Approved stack (kept)

- Video: LTX-2.5 distilled int8 + x2 latent upscaler + the LTX 2.3 Union-Control IC-LoRA
  (`ltx-2.3-22b-ic-lora-union-control-ref0.5`, still loaded by the LTX-2.5 pose/depth/canny
  graphs); MiniMax H3 (no H3 file was touched, including the `.part` downloads on f15);
  Wan-Animate-2 on worker-4090 (`wan_animate_2_int8_convrot`, `umt5_xxl_fp8_e4m3fn_scaled`,
  `clip_vision_h`, `Wan2_1_VAE_bf16`, lightx2v LoRA).
- Image: Krea 2 Turbo fp8 (+ `qwen3vl_4b_fp8_scaled`, `qwen_image_vae`), Z-Image Turbo fp8
  (+ `qwen_3_4b`, `ae.safetensors`, Fun ControlNet Union 2.1 full patch), FLUX.2 klein 4B
  (+ `qwen_3_4b_fp4_flux2`, `flux2-vae`), Qwen-Image-Edit-2511 GGUF + `qwen_2.5_vl_7b_fp8_scaled`
  + both Lightning LoRAs, Qwen-Image 2512 GGUF (still the `qwen_image_generate.json` template),
  Hunyuan3D / converter boxes (not touched), RMBG, DWPose, depth-anything, ESRGAN models.
- LoRAs in the /lora registry: Z-Detail-Slider, skin texture v4.5, AddMicroDetails Krea2,
  lenovo Krea2, the BTS LTX-2.5 and the bounce LTX 2.3 test LoRAs (plus H3 LoRAs other agents
  add). The Pony-family add-detail-xl and darth-vader-pxl were removed in the second batch.

"In use" was decided by grepping every template in `backend/renderfin/assets/workflows`
(local and the live release), both catalogues, the LoRA registry, `backend/*.py`,
`deploy/onlyrender/*.ps1` and the worker-4090 runtime scripts for each file name.

## What was retired and why

| Group | Files | Why |
|---|---|---|
| Old LTX | LTX 2.3 22B (distilled 1.1 transformer, distilled fp8 checkpoint, text projection, 2.3 VAEs), LTX-2 19B (+ distilled LoRA 384, camera-control LoRA, x2 spatial upscaler), LTXV 13B 0.9.8 (3 builds), 10Eros, Gemma 3 12B (fp4 and full, incl. the `models\LLM` copy), `t5xxl_fp8_e4m3fn` | Replaced by LTX-2.5; `RETIRED_MODEL_REPLACEMENTS` remaps the old names by name, no file needed. t5xxl was kept only for LTXV 13B. The 19B camera LoRA is only named by the non-dispatchable `animation_fitting` animal graphs. |
| LTX 2.3 style LoRAs | Crisp Enhance, Amateur Hour, Dual-Character, EditAnything, mvmt_lora_v2 (LTX23 build), Pixar Toon, DreamLTXV | No template uses them; not in the /lora registry. Removed from the catalogue. |
| Non-approved image | FLUX.2 dev fp8mixed + Mistral 3 small encoder, FLUX.2 klein base 9B + Qwen3-8B encoders (fp8mixed, fp4mixed), `z_image_turbo_bf16`, Qwen-Image 2512 / Edit 2511 fp8 safetensors | Nothing loads them (templates use the fp8 Z-Image, klein 4B and the GGUF Qwen builds). |
| Pony / SDXL | CyberRealistic Pony v18 (worker-4090 R:, Raptor X:), xinsir ControlNet Union SDXL, SDXL-only LoRAs on f15 (pixel-art-xl x2, ip-adapter faceid sdxl LoRA, StudioGhibli) | Pony is not in the approved stack. `gen_image_sdxl*.json` advertisements were withdrawn first (Raptor, worker-4090). |
| FLUX.1 leftovers | f12: flux1-schnell, FLUX.1 Union-Pro 2.0 fp8, clip_l, t5xxl_fp16, aidmaMJ6.1 LoRA; Raptor: Eye_Detail_Flux LoRA on D: | Pending part of the FLUX.1 removal. |
| Wan 2.x (not Animate-2) | f15: Wan2.2 Remix t2v high/low, Wan2.2 Seko 4-step LoRAs, NSFW Wan UMT5, `wan_2.1_vae`; worker-4090: Wan2.2-Animate-14B fp8 (Kijai), `wan_2.1_vae` | Wan was dropped; the Wan-Animate-2 graph uses other files. |

## Site and backend changes (commit 4b32676f, release `model-cleanup-20260923`)

- Curated catalogue (repo seed and live `/srv/autorig/data/var/ai-models/model_catalogue.json`,
  backup `model_catalogue.json.bak.model-cleanup-*`): removed the two FLUX.1 checkpoints, five
  FLUX.1 LoRAs, the six LTX 2.3 LoRAs, DreamLTXV, CyberRealistic Pony and Qwen-Image 2.1.
  The pickers now list only approved checkpoints; the LoRA picker shows only /lora-registry
  LoRAs for the chosen family.
- `ai_vision_api`: an image request with a control channel and no model now resolves the
  Z-Image family default (was Pony).
- `ai_pipelines_api`: `RETIRED_TOKENS` hides the five `gen_image_sdxl*` templates from the
  /models matrix. The templates stay in the tree (dormant SDXL code paths and tests load them).
- `renderfin-advertise.ps1` no longer adds `gen_image_sdxl.json`; `worker-4090-v037.ps1` no
  longer claims the five SDXL tokens. Renderfin registry: Raptor and worker-4090 re-posted
  without the SDXL tokens.

## Quarantine inventory

Sizes are bytes on the box. SHA-256 was computed on the box for files under 3 GB; larger
files show the hash the host publishes, marked "(source)", or "-" where none is published.
LoRAs that the /lora sync agent reports on f5/f15/Raptor/f12 went through its cleanup queue
instead (listed separately below); they land in `<ComfyUI root>\autorig_lora_trash\`.

### f5

13/13 moved, 198.89 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2-19b-distilled-fp8.safetensors` | 27078716346 | 8ae14327130c6ffdc87705b02c8e7654aa5c6d9a7f28a52d0acc1c30cb0d2932 (source) | https://huggingface.co/Lightricks/LTX-2 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2.3-22b-distilled-fp8.safetensors` | 29531884062 | d9646b6f2d5c42d337b23671634c43bfeece6989644f51b4a3aa088465ccd3b2 (source) | https://huggingface.co/Lightricks/LTX-2.3-fp8 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx10eros_v14_2989669.safetensors` | 29161843630 | - | https://civitai.com/models/2447875 (10Eros) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-dev-fp8.safetensors` | 15694279916 | b281bbb53b76d25a02285c148212b32daa6a57dfa46ce804c9bddba46f948c94 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled-fp8.safetensors` | 15694280140 | 111a3d07baa17f520e98b571e7916139ae0865c9a24b7534529d6b9e74264db3 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled.safetensors` | 28579183564 | 2c5f814744f04d8118e0b5bbfc0742655b51e41a6c090da66fde2936d651a9c9 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors` | 25226571988 | 0a1d7aac2b338e8ec7e832149f1dcf11c9323272482b1cca0673d229702370f0 (source) | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\LLM\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\ltx-2.3_text_projection_bf16.safetensors` | 2312149072 | 911d59bb4cb7708179c9a0045ea0fe41212ecfb77aed3a02702b7c0a8274911f | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_audio_vae_bf16.safetensors` | 364855188 | 5bc10fa4adecf99dda132d916e23048cbd56797702c5fa50eb5d2079048a38c3 | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_video_vae_bf16.safetensors` | 1452258578 | 01ea62d09bc139f95c5dee7b5c062ad6a3e6cd8be910a1983ac02e7eb5b8ee3b | https://huggingface.co/Kijai/LTX2.3_comfy | moved |

### f15

27/27 moved, 389.72 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2-19b-distilled-fp8.safetensors` | 27078716346 | 8ae14327130c6ffdc87705b02c8e7654aa5c6d9a7f28a52d0acc1c30cb0d2932 (source) | https://huggingface.co/Lightricks/LTX-2 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2.3-22b-distilled-fp8.safetensors` | 29531884062 | d9646b6f2d5c42d337b23671634c43bfeece6989644f51b4a3aa088465ccd3b2 (source) | https://huggingface.co/Lightricks/LTX-2.3-fp8 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx10eros_v14_2989669.safetensors` | 29161843630 | - | https://civitai.com/models/2447875 (10Eros) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-dev-fp8.safetensors` | 15694279916 | b281bbb53b76d25a02285c148212b32daa6a57dfa46ce804c9bddba46f948c94 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled-fp8.safetensors` | 15694280140 | 111a3d07baa17f520e98b571e7916139ae0865c9a24b7534529d6b9e74264db3 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled.safetensors` | 28579183564 | 2c5f814744f04d8118e0b5bbfc0742655b51e41a6c090da66fde2936d651a9c9 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\t5\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors` | 25226571988 | 0a1d7aac2b338e8ec7e832149f1dcf11c9323272482b1cca0673d229702370f0 (source) | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\flux-2-klein-base-9b-fp8.safetensors` | 9567278472 | - | https://huggingface.co/black-forest-labs/FLUX.2-klein-9B (Comfy repack) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\flux2_dev_fp8mixed.safetensors` | 35455599592 | - | https://huggingface.co/Comfy-Org/flux2-dev | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\z_image_turbo_bf16.safetensors` | 12309866400 | - | https://huggingface.co/Comfy-Org/z_image_turbo | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\qwen_image_2512_fp8_e4m3fn.safetensors` | 20430679144 | - | https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\qwen_image_edit_2511_fp8mixed.safetensors` | 20533762817 | - | https://huggingface.co/Comfy-Org/Qwen-Image_ComfyUI | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\Wan2.2_Remix_t2v_14b_high_lighting_v0.9_dyno.safetensors` | 14289668664 | - | Civitai Wan2.2 Remix (t2v 14B) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\Wan2.2_Remix_t2v_14b_low_lighting_v0.9.safetensors` | 14289668960 | - | Civitai Wan2.2 Remix (t2v 14B) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\latent_upscale_models\ltx-2-spatial-upscaler-x2-1.0.safetensors` | 995765578 | 3160fabf8edf0bc4dd8de40353a180813b111ce586b655ad54af9a7b8c6736de | https://huggingface.co/Lightricks/LTX-2 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\LLM\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\gemma_3_12B_it.safetensors` | 24379468890 | 56eaa964a0d9325d2dc9ecaf7759bfaf0fac78ae36c789bed6e03e275a3729ec (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\ltx-2.3_text_projection_bf16.safetensors` | 2312149072 | 911d59bb4cb7708179c9a0045ea0fe41212ecfb77aed3a02702b7c0a8274911f | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\mistral_3_small_flux2_fp8.safetensors` | 18034640095 | - | https://huggingface.co/Comfy-Org/flux2-dev | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\nsfw_wan_umt5-xxl_fp8_scaled.safetensors` | 6735887993 | - | Civitai NSFW Wan UMT5 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\qwen_3_8b_fp8mixed.safetensors` | 8664848742 | - | https://huggingface.co/black-forest-labs/FLUX.2-klein-9B (Comfy repack) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_audio_vae_bf16.safetensors` | 364855188 | 5bc10fa4adecf99dda132d916e23048cbd56797702c5fa50eb5d2079048a38c3 | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_video_vae_bf16.safetensors` | 1452258578 | 01ea62d09bc139f95c5dee7b5c062ad6a3e6cd8be910a1983ac02e7eb5b8ee3b | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\wan_2.1_vae.safetensors` | 253815318 | 2fc39d31359a4b0a64f55876d8ff7fa8d780956ae2cb13463b0223e15148976b | https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged | moved |

### Raptor

24/24 moved, 213.83 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2-19b-distilled-fp8.safetensors` | 27078716346 | 8ae14327130c6ffdc87705b02c8e7654aa5c6d9a7f28a52d0acc1c30cb0d2932 (source) | https://huggingface.co/Lightricks/LTX-2 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2.3-22b-distilled-fp8.safetensors` | 29531884062 | d9646b6f2d5c42d337b23671634c43bfeece6989644f51b4a3aa088465ccd3b2 (source) | https://huggingface.co/Lightricks/LTX-2.3-fp8 | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx10eros_v14_2989669.safetensors` | 29161843630 | - | https://civitai.com/models/2447875 (10Eros) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled-fp8.safetensors` | 15694280140 | 111a3d07baa17f520e98b571e7916139ae0865c9a24b7534529d6b9e74264db3 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors` | 25226571988 | 0a1d7aac2b338e8ec7e832149f1dcf11c9323272482b1cca0673d229702370f0 (source) | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\LLM\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\ltx-2.3_text_projection_bf16.safetensors` | 2312149072 | 911d59bb4cb7708179c9a0045ea0fe41212ecfb77aed3a02702b7c0a8274911f | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\qwen_3_8b_fp4mixed.safetensors` | 6802593327 | - | https://huggingface.co/black-forest-labs/FLUX.2-klein-9B (Comfy repack) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\qwen_3_8b_fp8mixed.safetensors` | 8664848742 | - | https://huggingface.co/black-forest-labs/FLUX.2-klein-9B (Comfy repack) | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_audio_vae_bf16.safetensors` | 364855188 | 5bc10fa4adecf99dda132d916e23048cbd56797702c5fa50eb5d2079048a38c3 | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_video_vae_bf16.safetensors` | 1452258578 | 01ea62d09bc139f95c5dee7b5c062ad6a3e6cd8be910a1983ac02e7eb5b8ee3b | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `X:\FleetModels\checkpoints\cyberrealisticPony_v180Coreshift_2764472.safetensors` | 6938041288 | - | https://civitai.com/models/443821 (CyberRealistic Pony v18) | moved |
| `X:\FleetModels\checkpoints\ltx10eros_v14_2989633.safetensors` | 1699829528 | 99d4c620ac3375e134624ab5dd2fcc66560a1a5397f5034b5be25210cc4e8b8c | https://civitai.com/models/2447875 (10Eros) | moved |
| `X:\FleetModels\checkpoints\ltx10eros_v14_2989669.safetensors` | 29161843630 | - | https://civitai.com/models/2447875 (10Eros) | moved |
| `X:\FleetModels\loras\ltx\AmateurHour_01_rank16.safetensors` | 176493392 | e535e0bea044a678b8cab02c5d7e3ce2ac5add3aa2ac0175d812cb8098fd53d2 | https://civitai.com/models/2530917?modelVersionId=2844417 | moved |
| `X:\FleetModels\loras\ltx\DreamLTXV.safetensors` | 190914984 | 3d74deb41b38adc55f98bf93e6c098b638cb4a45d9e7ad49eff671d41fcc9f51 | https://civitai.com/models/1264762?modelVersionId=1426312 | moved |
| `X:\FleetModels\loras\ltx\ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | https://huggingface.co/Lightricks/LTX-2-19b-LoRA-Camera-Control-Static | moved |
| `X:\FleetModels\loras\ltx\LTX2.3-IC-LORA-Dual-Character.safetensors` | 327287347 | b6c3199e2c95eb0aad0cea4f3e8dfa33e8f6966d85bc79e3994faf5a57406103 | https://civitai.com/models/2500098?modelVersionId=2810376 | moved |
| `X:\FleetModels\loras\ltx\LTX2.3_Crisp_Enhance.safetensors` | 705198392 | 020529377ce07b1235d8050505dd38adc3bc9dc49a191f318a7ccf3921354053 | https://civitai.com/models/2535622?modelVersionId=2849716 | moved |
| `X:\FleetModels\loras\ltx\ltx23_edit_anything_global_rank128_v1_9000steps_adamw.safetensors` | 1308756416 | 36721b3988c468afbec2f5bc52eb534fc6a3398fcf150303eba24f27d9da2974 | https://civitai.com/models/2553102?modelVersionId=2869279 | moved |
| `X:\FleetModels\loras\ltx\mvmt_lora_v2_600.safetensors` | 674249616 | 3cd5aeeff4e079510527650af4c45c1ec8a51495996de34c8d155fe35b6ed6a5 | https://civitai.com/models/2734359?modelVersionId=3087364 | moved |
| `X:\FleetModels\loras\ltx\Pixar_Toon.safetensors` | 352679872 | 7606e01d7e9bccbd983b720062e3ce6118f3efaa4c233f5448dc78eebf5e140b | https://civitai.com/models/2536130?modelVersionId=2850271 | moved |

### f12

3/7 moved by this cleanup (47.67 GB); the 4 FLUX.1 files were already parked in the same folder by the image agent. 83.62 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx-2-19b-distilled-fp8.safetensors` | 27078716346 | 8ae14327130c6ffdc87705b02c8e7654aa5c6d9a7f28a52d0acc1c30cb0d2932 (source) | https://huggingface.co/Lightricks/LTX-2 | moved |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltxv-13b-0.9.8-distilled-fp8.safetensors` | 15694280140 | 111a3d07baa17f520e98b571e7916139ae0865c9a24b7534529d6b9e74264db3 (source) | https://huggingface.co/Lightricks/LTX-Video | moved |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\clip\clip_l.safetensors` | 246144152 | 660c6f5b1abae9dc498ac2d21e1347d2abdb0cf6c0c0c8576cd796491d9a6cdd (source) | https://huggingface.co/comfyanonymous/flux_text_encoders | moved by the image agent to `C:\_retired_20260923\models\...` |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp16.safetensors` | 9787841024 | 6e480b09fae049a72d2a8c5fbccb8d3e92febeb233bbe9dfe7256958a9167635 (source) | https://huggingface.co/comfyanonymous/flux_text_encoders | moved by the image agent to `C:\_retired_20260923\models\...` |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8.safetensors` | 2140902936 | 393fc2a298b93ffe39f2db3f0d2ce11dfba62d44b7aa3c1dd3380d4a1be04deb (source) | https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 (fp8) | moved by the image agent to `C:\_retired_20260923\models\...` |
| `C:\AI\ComfyUI_windows_portable\ComfyUI\models\unet\flux1-schnell.safetensors` | 23782506688 | 9403429e0052277ac2a87ad800adece5481eecefd9ed334e1f348723621d2a0a (source) | https://huggingface.co/black-forest-labs/FLUX.1-schnell | moved by the image agent to `C:\_retired_20260923\models\...` |

### worker-4090

20/20 moved, 111.81 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `R:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\CyberRealisticPony_V18.0_F16.safetensors` | 6938041288 | - | https://civitai.com/models/443821 (CyberRealistic Pony v18) | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\ltx10eros_v14_2989669.safetensors` | 29161843630 | - | https://civitai.com/models/2447875 (10Eros) | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\controlnet\xinsir-controlnet-union-sdxl-1.0.safetensors` | 2512030408 | a9e13fd61f3193887791c8a0dd07a07202174dc47d5ddaea94ea1344f07c7467 | https://huggingface.co/xinsir/controlnet-union-sdxl-1.0 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors` | 25226571988 | 0a1d7aac2b338e8ec7e832149f1dcf11c9323272482b1cca0673d229702370f0 (source) | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\Wan2_2-Animate-14B_fp8_e4m3fn_scaled_KJ.safetensors` | 18401760586 | - | https://huggingface.co/Kijai/WanVideo_comfy_fp8_scaled | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\AmateurHour_01_rank16.safetensors` | 176493392 | e535e0bea044a678b8cab02c5d7e3ce2ac5add3aa2ac0175d812cb8098fd53d2 | https://civitai.com/models/2530917?modelVersionId=2844417 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\DreamLTXV.safetensors` | 190914984 | 3d74deb41b38adc55f98bf93e6c098b638cb4a45d9e7ad49eff671d41fcc9f51 | https://civitai.com/models/1264762?modelVersionId=1426312 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | https://huggingface.co/Lightricks/LTX-2-19b-LoRA-Camera-Control-Static | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\LTX2.3-IC-LORA-Dual-Character.safetensors` | 327287347 | b6c3199e2c95eb0aad0cea4f3e8dfa33e8f6966d85bc79e3994faf5a57406103 | https://civitai.com/models/2500098?modelVersionId=2810376 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\LTX2.3_Crisp_Enhance.safetensors` | 705198392 | 020529377ce07b1235d8050505dd38adc3bc9dc49a191f318a7ccf3921354053 | https://civitai.com/models/2535622?modelVersionId=2849716 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\ltx23_edit_anything_global_rank128_v1_9000steps_adamw.safetensors` | 1308756416 | 36721b3988c468afbec2f5bc52eb534fc6a3398fcf150303eba24f27d9da2974 | https://civitai.com/models/2553102?modelVersionId=2869279 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\mvmt_lora_v2_600.safetensors` | 674249616 | 3cd5aeeff4e079510527650af4c45c1ec8a51495996de34c8d155fe35b6ed6a5 | https://civitai.com/models/2734359?modelVersionId=3087364 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\loras\Pixar_Toon.safetensors` | 352679872 | 7606e01d7e9bccbd983b720062e3ce6118f3efaa4c233f5448dc78eebf5e140b | https://civitai.com/models/2536130?modelVersionId=2850271 | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors` | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d (source) | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\ltx-2.3_text_projection_bf16.safetensors` | 2312149072 | 911d59bb4cb7708179c9a0045ea0fe41212ecfb77aed3a02702b7c0a8274911f | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\t5xxl_fp8_e4m3fn.safetensors` | 4893934904 | - | https://huggingface.co/comfyanonymous/flux_text_encoders | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_audio_vae_bf16.safetensors` | 364855188 | 5bc10fa4adecf99dda132d916e23048cbd56797702c5fa50eb5d2079048a38c3 | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\vae\LTX23_video_vae_bf16.safetensors` | 1452258578 | 01ea62d09bc139f95c5dee7b5c062ad6a3e6cd8be910a1983ac02e7eb5b8ee3b | https://huggingface.co/Kijai/LTX2.3_comfy | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\vae\wan_2.1_vae.safetensors` | 253815318 | 2fc39d31359a4b0a64f55876d8ff7fa8d780956ae2cb13463b0223e15148976b | https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged | moved |

### f5 - second batch (owner-approved extras)

13/13 moved, 18.27 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\config.json` | 1395 | 39e0f775d681c3d62f8cee5967c77147a09d201f67d4d8f36cf5c15abced929e |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\generation_config.json` | 124 | 78619a2126645647c540fa0b192fe291cc0bf5546deebf52a0736280a3b08eef |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00001-of-00003.safetensors` | 4938985248 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00002-of-00003.safetensors` | 4947390768 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00003-of-00003.safetensors` | 4239524472 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model.safetensors.index.json` | 73152 | 3bcc82d6085573aaa02a02ad8c7973112d037fc8988699bb8d0e446a043364a2 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\special_tokens_map.json` | 552 | 4859e5dbde90e059988a0a2136d8df3f2773d4d2fc4c4543690028f0b2166e7f |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\tokenizer.model` | 499723 | 9e556afd44213b6bd1be2b850ebbbd98f5481437a8021afaf58ee7fb1818d347 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\tokenizer_config.json` | 936 | b9dbbc4e94fa11210a21800939bd17da1e91b43f083833fb031b394111de6a9a |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\random_mask_brushnet_ckpt\config.json` | 1384 | 8cc33f7680a934558d5566409d93fda237f28330219e70482b5a142fbdcebc20 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\random_mask_brushnet_ckpt\diffusion_pytorch_model.safetensors` | 2475354520 | 089f02196144dc2713d6d833355c1f11a73460155c9b33f86c9e2e73b4a77583 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\segmentation_mask_brushnet_ckpt\config.json` | 1381 | c3d8a4911c38c79a7b771b98daa1e279669766afd9cf39c1b6ba302c77922ebf |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\segmentation_mask_brushnet_ckpt\diffusion_pytorch_model.safetensors` | 1669734400 | 02f9849340d41f9e27b16facd0c1debf1504aedb590f73638b1d3711e8deb61d |  | moved |

### f15 - second batch (owner-approved extras)

30/30 moved, 27.78 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\config.json` | 1395 | 39e0f775d681c3d62f8cee5967c77147a09d201f67d4d8f36cf5c15abced929e |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\generation_config.json` | 124 | 78619a2126645647c540fa0b192fe291cc0bf5546deebf52a0736280a3b08eef |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00001-of-00003.safetensors` | 4938985248 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00002-of-00003.safetensors` | 4947390768 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model-00003-of-00003.safetensors` | 4239524472 | - |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\model.safetensors.index.json` | 73152 | 3bcc82d6085573aaa02a02ad8c7973112d037fc8988699bb8d0e446a043364a2 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\special_tokens_map.json` | 552 | 4859e5dbde90e059988a0a2136d8df3f2773d4d2fc4c4543690028f0b2166e7f |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\tokenizer.model` | 499723 | 9e556afd44213b6bd1be2b850ebbbd98f5481437a8021afaf58ee7fb1818d347 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\llava-v1.5-7b-finetune-clean\tokenizer_config.json` | 936 | b9dbbc4e94fa11210a21800939bd17da1e91b43f083833fb031b394111de6a9a |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\random_mask_brushnet_ckpt\config.json` | 1384 | 8cc33f7680a934558d5566409d93fda237f28330219e70482b5a142fbdcebc20 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\random_mask_brushnet_ckpt\diffusion_pytorch_model.safetensors` | 2475354520 | 089f02196144dc2713d6d833355c1f11a73460155c9b33f86c9e2e73b4a77583 |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\segmentation_mask_brushnet_ckpt\config.json` | 1381 | c3d8a4911c38c79a7b771b98daa1e279669766afd9cf39c1b6ba302c77922ebf |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\inpaint\brushnet\segmentation_mask_brushnet_ckpt\diffusion_pytorch_model.safetensors` | 1669734400 | 02f9849340d41f9e27b16facd0c1debf1504aedb590f73638b1d3711e8deb61d |  | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid-plusv2_sd15.bin` | 156558509 | 26d0d86a1d60d6cc811d3b8862178b461e1eeb651e6fe2b72ba17aa95411e313 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid-plusv2_sdxl.bin` | 1487555181 | c6945d82b543700cc3ccbb98d363b837e9c596281607857c74b713a876daf5fb | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid-portrait-v11_sd15.bin` | 64586623 | a48cb4f89ed18e02c6000f65aa9efec452e87eaed4a1bc9fcf4a460c8d0e3bc6 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid-portrait_sdxl.bin` | 749822515 | 5631ce7824cdafd2db37c5e85b985730a95ff59c5b4fc80c2b79b0bee5711512 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid-portrait_sdxl_unnorm.bin` | 1009523411 | 220bb86e205393a3d0411631cb473caddbf35fd371be2905ca9008818170db55 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-faceid_sd15.bin` | 96740574 | 201344e22e6f55849cf07ca7a6e53d8c3b001327c66cb9710d69fd5da48a8da7 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-full-face_sd15.safetensors` | 43592352 | f4a17fb643bf876235a45a0e87a49da2855be6584b28ca04c62a97ab5ff1c6f3 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-plus-face_sdxl_vit-h.safetensors` | 847517512 | 677ad8860204f7d0bfba12d29e6c31ded9beefdf3e4bbd102518357d31a292c1 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-plus_sd15.safetensors` | 98183288 | a1c250be40455cc61a43da1201ec3f1edaea71214865fb47f57927e06cbe4996 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter-plus_sdxl_vit-h.safetensors` | 847517512 | 3f5062b8400c94b7159665b21ba5c62acdcd7682262743d7f2aefedef00e6581 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sd15.safetensors` | 44642768 | 289b45f16d043d0bf542e45831f971dcdaabe18b656f11e86d9dfba7e9ee3369 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sd15_light.safetensors` | 44642768 | 0747d08db670535bfa286452a77d93cebad5c677b46d038543f9f2de8690bb26 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sd15_light_v11.bin` | 44642977 | 350b63a57847c163e2e984b01090f85ffe60eaae20f32b2b2c9e1ccc7ddd972b | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sd15_vit-G.safetensors` | 46215640 | a26f736af07bb341a83dfea23713531d0575760e8ed947c68cb31a4c62d9c90b | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sdxl.safetensors` | 702585376 | ba1002529e783604c5f326d49f0122025392d1d20ac8d573b3eeb3e6dea4ebb6 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\ipadapter\ip-adapter_sdxl_vit-h.safetensors` | 698391064 | ebf05d918348aec7abb02a5e9ecef77e0aaea6914a5c4ea13f50d45eb1681831 | https://huggingface.co/h94/IP-Adapter / IP-Adapter-FaceID | moved |
| `D:\ComfyUI_windows_portable\ComfyUI\models\clip_vision\CLIP-ViT-H-14-laion2B-s32B-b79K.safetensors` | 2528373448 | 6ca9667da1ca9e0b0f75e46bb030f7e011f44f86cbfb8d5a36590fcd7507b030 | https://huggingface.co/laion/CLIP-ViT-H-14-laion2B-s32B-b79K | moved |

### worker-4090 - second batch (owner-approved extras)

4/4 moved, 45.53 GB listed.

| Path (original) | Bytes | SHA-256 | Source | State |
|---|---:|---|---|---|
| `R:\ComfyUI_windows_portable\ComfyUI\models\foley\hunyuanvideo_foley.safetensors` | 10268151840 | - | https://huggingface.co/tencent/HunyuanVideo-Foley | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\foley\synchformer_state_dict_fp16.safetensors` | 474981066 | 4876e7dc86cd255bcf79540d93838362aed2e8a9d6f4574a92728c9fc699f511 | https://huggingface.co/tencent/HunyuanVideo-Foley | moved |
| `R:\ComfyUI_windows_portable\ComfyUI\models\foley\vae_128d_48k_fp16.safetensors` | 743210820 | 216fdd5a4c9774417b209a5d1754db5326c9f47375b232cce1333c13b3f239ab | https://huggingface.co/tencent/HunyuanVideo-Foley | moved |
| `C:\AIModels\diffusion_models\minimax_h3_fl2va_int8_convrot.safetensors` | 34038892334 | 7ad4c73e6e378b822ffd1629f27f632d3787d95f5e468e3af958f98c58df96a5 (source) | https://huggingface.co/Comfy-Org/MiniMax-H3 (unpruned int8; pruned build kept) | moved |

### LoRAs moved by the /lora sync agent (cleanup queue)

Queued in `registry.json` → `cleanup`, executed by each box's "AutoRig LoRA Sync" task,
which moved them to `<ComfyUI root>\autorig_lora_trash\20260923-HHMMSS_<name>` (same drive).
All four queues drained by 23:00Z.

| Box | File (in `models\loras`) | Bytes | SHA-256 | Source |
|---|---|---:|---|---|
| f5 | `ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | https://huggingface.co/Lightricks/LTX-2-19b-LoRA-Camera-Control-Static |
| f15 | `ltx-2-19b-distilled-lora-384.safetensors` | 7674558424 | 2718f89582003cbb5b616635f18c091641917a3f3e5a2f2ad0fb3d5fdd153534 | https://huggingface.co/Lightricks/LTX-2 |
| f15 | `ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | as above |
| f15 | `Wan2.2-T2V-A14B-4steps-lora-rank64-Seko-V2.0-high.safetensors` | 1226977424 | 78cc2c9b44aca9ded8f69c6619639edca7459308051161253c3aa04ac6169a58 | https://huggingface.co/lightx2v/Wan2.2-Lightning |
| f15 | `Wan2.2-T2V-A14B-4steps-lora-rank64-Seko-V2.0-low.safetensors` | 1226977424 | 6bb498f926e71217106c3642e75273580c1de2dec0a6902cbae8e83d2402d5cf | as above |
| f15 | `1.5\pixel-art-xl-v1.0.safetensors` | 170543052 | 4234637cb80c998f41e348e6a6cb6bc20d8d038b2b0f256b6129b3b5e353eef7 | https://huggingface.co/nerijs/pixel-art-xl |
| f15 | `1.5\pixel-art-xl-v1.1.safetensors` | 170543052 | bbf3d8defbfb3fb71331545225c0cf50c74a748d2525f7c19ebb8f74445de274 | as above |
| f15 | `ipadapter\ip-adapter-faceid-plusv2_sdxl_lora.safetensors` | 371842896 | f24b4bb2dad6638a09c00f151cde84991baf374409385bcbab53c1871a30cb7b | https://huggingface.co/h94/IP-Adapter-FaceID |
| f15 | `SDXL\StudioGhibli.Redmond-StdGBRRedmAF-StudioGhibli.safetensors` | 228450868 | 516fe22303b8a24f800a0a24a8f4c864121411b7261206c406183c1103b7bf37 | https://huggingface.co/artificialguybr/StudioGhibli.Redmond-V2 |
| Raptor | `Eye_Detail_Flux_Lora_-_Inpainting.safetensors` | 19257544 | 3107d1ffa5d185168fa33af4386dfb2b6ba813968e72f3c6de16ac7a7fb6bf7a | https://civitai.com/models/263104?modelVersionId=1001942 |
| Raptor | `ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | as above |
| f12 | `aidmaMJ6.1-FLUX-v0.5.safetensors` | 76853160 | ecb64944ab1ccbe62a7bcf1d149d9add41de9392d236542587a9cf326fdeb5a4 | https://civitai.com/models/646411?modelVersionId=1249246 |
| f12 | `ltx-2-19b-lora-camera-control-static.safetensors` | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | as above |

## Verification

- Every renderfin box (f5, f15, Raptor, f12, worker-4090) re-checked against `object_info`
  after its moves: every advertised template, plus the templates that ride the `gen_image.json`
  and canny tokens (T-pose, inpaint, enhance, Qwen-Image), still finds every node class and every
  model file (`wfmodels.py`, run from the VPS). ComfyUI re-lists model folders on each
  `object_info`, no restart was needed (Raptor's checkpoint list came back empty right after).
  One pre-existing gap, not caused by this cleanup: f12 now advertises the canny token, whose
  Qwen-Image riders need `UnetLoaderGGUF` and `qwen_2.5_vl_7b_fp8_scaled`, which f12 never had
  (Qwen-Image dispatch is filtered by `model_eligibility`, so no Qwen job is sent there).
- /api/ai/model-catalogue: checkpoints = klein 4B, MiniMax H3, LTX-2.5, Z-Image, Krea 2,
  Qwen-Image 2512 / Edit 2511; LoRAs = the eight /lora registry entries only.
  /api/ai/model-settings?service=image&control_channel=pose → Z-Image.
- /api/ai/pipelines: no `gen_image_sdxl*` rows. (`gen_image_flux1_schnell.json` still shows
  from the 24 h job history and drops out on its own.)
- Production smoke jobs after the moves (public `/api/image`, 768², same prompt):
  Krea 2 default `0e3d45a4-6118-43aa-8997-4aed74380a21` (Raptor, 24 s),
  Z-Image `2b0da22f-19bc-459f-a829-78b17c845329` (f5, 2 s warm),
  FLUX.2 klein 4B `15b14955-ce63-450f-a3a2-ec962ca06c56` (Raptor, 77 s).
  LTX-2.5 video (image-to-video, 49 frames) `fd84d50f-ee66-48ed-ab54-cc6b4a599214`
  (f12, `gen_animation_ltx25_by_url.json`, 72 s) after f12's moves.

## Second batch (owner: "да, удали всё", 2026-09-24)

Moved the same way (queue idle before each file), tables above marked "second batch": LLaVA 1.5 7B
and BrushNet on f5/f15; the 15 IP-Adapter files and the IP-Adapter image encoder
`clip_vision\CLIP-ViT-H-14-laion2B-s32B-b79K` on f15; HunyuanVideo-Foley (3 files) on worker-4090 R:;
the unpruned MiniMax H3 `C:\AIModels\diffusion_models\minimax_h3_fl2va_int8_convrot.safetensors`
(34.04 GB) to `C:\_retired_20260923` on worker-4090. Before that move: no template, catalogue entry or
backend path loads the unpruned file (the catalogue lists it only under `legacy_files`, and
`ai_model_defaults` maps the old name to the pruned build); `C:\LoraTraining\lensflare_h3
un_train.ps1`
already trains on the pruned base, and `C:\LoraTraining	ools\comfy_h3_eval.py` now defaults
`--unet` to the pruned file (it defaulted to the unpruned one). worker-4090 re-checked after the move:
all 17 advertised templates complete.

The Pony-family LoRAs `darth-vader-pxl` (civitai-1071192) and `add-detail-xl` (civitai-135867) were
removed from the /lora registry through the manager's remove flow (state `removed`, mirror blob
deleted, boxes kicked); the sync agents on f5, f15, Raptor and f12 moved their copies to
`autorig_lora_trash`. The registry now holds six LoRAs.

## Not moved: candidates for the owner

| Box | File | Size | Why it was left |
|---|---|---:|---|
| f15 | `text_encoders\qwen_3_4b_fp4_flux2.safetensors.part.bad-*`, `vae\flux2-vae.safetensors.bad-*` | 4.3 GB | Broken downloads left by the image agent. |
| Raptor | `vae\.previous\flux2-vae.d64f3a68.safetensors` | 0.3 GB | Image agent's backup. |
| code | `gen_image_sdxl*.json` templates, SDXL family routing in `ai_model_defaults.py` / `ai_vision_api.py`, `FORWARD_COMPATIBLE_LORAS` | - | Dormant; removing them touches ~14 test files. |

## Purge (owner, when satisfied)

Nothing is deleted until these are run. One line per box; each frees the GB shown.

Run on each box (farm boxes: from the VPS with
`sudo -n ssh -i /srv/autorig/secrets/ssh/renderfin_farm_tunnel -p <48488 f5 | 48588 f15 | 48288 Raptor> <user>@5.129.157.224`,
f12: `ssh f12` from the owner's PC; worker-4090 is the owner's PC). The `autorig_lora_trash`
folders hold only this cleanup's LoRAs (checked 2026-09-24 00:30Z). Sizes measured after the last move.

| Box | Folders | GB |
|---|---|---:|
| f5 | `D:\_retired_20260923` (217.16), `D:\ComfyUI_windows_portable\autorig_lora_trash` (2.73) | 219.88 |
| f15 | `D:\_retired_20260923` (417.50), `D:\ComfyUI_windows_portable\autorig_lora_trash` (13.80) | 431.30 |
| Raptor | `D:\_retired_20260923` (170.08), `X:\_retired_20260923` (43.75), `D:\ComfyUI_windows_portable\autorig_lora_trash` (2.75) | 216.58 |
| f12 | `C:\_retired_20260923` (83.62, incl. 35.96 FLUX.1 leftovers the image agent parked there), `C:\AI\ComfyUI_windows_portable\autorig_lora_trash` (2.63) | 86.26 |
| worker-4090 | `R:\_retired_20260923` (123.30), `C:\_retired_20260923` (34.04, the unpruned H3) | 157.33 |
| **Total** | | **1111.35** |

One purge script works on every box: it removes whichever of these folders exist.

```powershell
foreach ($p in 'C:\_retired_20260923','D:\_retired_20260923','R:\_retired_20260923','X:\_retired_20260923',
               'D:\ComfyUI_windows_portable\autorig_lora_trash','C:\AI\ComfyUI_windows_portable\autorig_lora_trash') {
  if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Recurse -Force; "removed $p" } }
```

To restore a file, move it back, e.g.
`Move-Item 'D:\_retired_20260923\ComfyUI_windows_portable\ComfyUI\models\<dir>\<file>' 'D:\ComfyUI_windows_portable\ComfyUI\models\<dir>\'`.
