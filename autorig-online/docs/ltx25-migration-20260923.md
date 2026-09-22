# LTX-2.5 migration and MiniMax H3 (2026-09-23)

Owner decision: every old LTX video model is replaced by **LTX-2.5 distilled**;
old files are deleted only after LTX-2.5 is proven, and (added the same day)
only when the owner says so. **MiniMax H3** is the premium video engine. Wan is
dropped. Tests use only the approved stack below; no comparison downloads.

## 1. Approved artifacts (pinned)

The official `Lightricks/LTX-2.5` repo is gated (`gated=auto`, our token has not
accepted the terms), so the files were taken from the public ComfyUI repack
`comfyicu/LTX-2.5`. The same sha256 is published by three independent mirrors
(comfyicu, lxxxy6, yuvraj108c), which is the integrity check used.

| File | Folder | Bytes | sha256 | Source |
|---|---|---|---|---|
| ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors | diffusion_models | 21504034224 | c4279eeff115cbeaca494bd2183e7d768c38fe85a184dc6afbb7159157c44334 | https://huggingface.co/comfyicu/LTX-2.5/resolve/main/diffusion_models/ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors |
| gemma4-12b-with-proj-ltx-2.5-comfy-int8-convrot.safetensors | text_encoders | 15372971786 | 09a89e084de1a149c3de60cfe9dfd3e5161967eb09eea39e806fcdeffdd568de | …/comfyicu/LTX-2.5/resolve/main/text_encoders/… |
| ltx-2.5-video-vae-bf16.safetensors | vae | 1472223346 | 847e14ca7f3355debca0cea4eaa24ac0fbcdf0061da054ac89ca638a869ddba3 | …/comfyicu/LTX-2.5/resolve/main/vae/… |
| ltx-2.5-audio-vae-bf16.safetensors | vae | 364866540 | c52733d37f6a7fb7949c3dc0fb468c6cb2169e4d836983a73babb9f0d54837a5 | …/comfyicu/LTX-2.5/resolve/main/vae/… |
| ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors (HQ only) | latent_upscale_models | 995778752 | eb5a71fe4068ee87ccdb1c3aa635e547ca76bd2d30ae20ae889f2c325c0677e8 | …/comfyicu/LTX-2.5/resolve/main/latent_upscale_models/… |
| ltx-2.3-22b-ic-lora-union-control-ref0.5.safetensors (pose/depth/canny) | loras | 654465352 | a1b888a87f661d27f08b394ae559e8e1050be33900bcc36a5cdf659e48f88d18 | https://huggingface.co/Lightricks/LTX-2.3-22b-IC-LoRA-Union-Control (not gated) |

One int8-convrot build runs on every class of card (no GGUF): the 12 GB
3080 Ti (f12) streams it with ComfyUI's dynamic offload in 50 s per clip.

MiniMax H3 (`Comfy-Org/MiniMax-H3`, turbo LoRA `lightx2v/Minimax-h3-Turbo`):

| File | Folder | Bytes | sha256 |
|---|---|---|---|
| minimax_h3_fl2va_int8_convrot.safetensors | diffusion_models | 34038892334 | 7ad4c73e6e378b822ffd1629f27f632d3787d95f5e468e3af958f98c58df96a5 |
| qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors | text_encoders | 15687142551 | 35a88d51044231fe332301d7a62aa81e3f2cba62febeb446e2c1e3e0ef76f2c6 |
| minimax_h3_video_vae_fp16.safetensors | vae | 5207808496 | 7c1f131492e7eddacaac9069a61b81bdd39de5cc96561e677c5eab1cdce5e522 |
| minimax_h3_audio_vae_fp32.safetensors | vae | 605254808 | 8e505d95dd1561d47abd43d4238fd40d9bb1ae9e147ed0a4cba778d76ae4db48 |
| minimax_h3_fl2v_turbo_4step_v1.2_768p_comfyui_bf16.safetensors | loras | 1956193000 | c8168ebc17bbacc4296103dda2fec1ba85b24392fa08cf2bfbcef0cff0dc3cc8 |

No custom nodes are needed: ComfyUI 0.37 has the LTX-2.5 and H3 nodes natively
(`LTXVImgToVideo*`, `LTXVLatentUpsampler`, `MiniMaxH3ImageToVideo`,
`MiniMaxH3AddGuide`); ComfyUI-LTXVideo supplies `LTXVTiledVAEDecode`,
`LTXICLoRALoaderModelOnly`, `LTXAddVideoICLoRAGuide`.

On worker-4090 the new weights live on `C:\AIModels\<folder>` (R: is full),
mapped by the `aimodels_c` block of `R:\autorig\.runtime\onlyrender\extra_model_paths.yaml`.

## 2. Templates and tokens

| Token (file in `backend/renderfin/assets/workflows`) | Runs |
|---|---|
| `gen_animation_ltx25_by_url.json` | LTX-2.5 single stage, 8 steps, CFG 1, optional end frame |
| `gen_animation_ltx25_hq_by_url.json` | LTX-2.5 two stage: half-size pass, x2 latent upscale, 3-step refine |
| `gen_animation_by_url.json`, `gen_animation_ltx23_by_url.json`, `gen_animation_ltx10eros_by_url.json` | identical copies of the single-stage graph (historical names kept for saved graphs and callers) |
| `gen_animation_hq_by_url.json` | identical copy of the two-stage graph |
| `gen_video_ltx23_{pose,depth,control}_by_url.json` | LTX-2.5 + 2.3 Union-Control IC-LoRA (pose / depth / canny) |
| `gen_video_minimax_h3_by_url.json` | MiniMax H3 fl2va, 4-step turbo LoRA, optional end frame, stereo audio |

Backend: `ai_model_defaults.RETIRED_MODEL_REPLACEMENTS` maps every retired LTX
checkpoint name to the LTX-2.5 file (backend only; not a renderfin alias, so a
box that still holds an old file is never handed it). LTX 2.3 LoRAs are
accepted on LTX-2.5 (`FORWARD_COMPATIBLE_LORAS`). `quality: hq` on an LTX-2.5
checkpoint picks the two-stage graph. Video control forces the LTX-2.5
transformer.

## 3. Measurements

Same image and prompt everywhere (960x540 delivery, 97 frames = 4.04 s, 24 fps,
audio on). "cold" includes loading the weights.

| Box | GPU / RAM | Workflow | Run | Peak VRAM | Peak system RAM |
|---|---|---|---|---|---|
| f12 | 3080 Ti 12 GB / 32 GB | LTX-2.5 standard | 90 s cold, 50 s warm | 11.7 GB | 27.7 GB (4.0 GB free at worst) |
| f12 | | LTX-2.5 HQ two-stage | 57 s (bench), 210 s wall via prod incl. load | 11.8 GB | 25-28 GB |
| f12 | | LTX-2.5 pose control (97 f DWPose) | **box went down** (reboot 19:24Z) | – | RAM exhaustion suspected |
| worker-4090 | 4090 24 GB / 64 GB | LTX-2.5 standard | 46 s cold | 23.6 GB (allocator) | 35 GB |
| worker-4090 | | LTX-2.5 HQ | 63 s | 23.6 GB | 38 GB |
| worker-4090 | | LTX-2.5 pose control (prod) | 140 s wall | | |
| worker-4090 | | MiniMax H3 (107 f) | 578 s cold, 41 s warm | 23.6 GB | 48 GB |
| f5, f15 | 3070 Ti 8 GB / 32 GB | LTX-2.5 | pending (weights still downloading) | | |
| Raptor | 3080 Ti 12 GB / 64 GB | LTX-2.5, H3 | pending (weights still downloading) | | |

## 4. Production proof (public `POST /api/video`)

| Task | Box | Workflow | Wall |
|---|---|---|---|
| 22f4356c-66be-4c34-a7a7-1d2351117ff5 | worker-4090 | gen_animation_ltx25_by_url.json | 60 s |
| 33e36d32-2522-4d3f-971d-1f204bfb461b | worker-4090 | gen_animation_ltx25_hq_by_url.json | 75 s |
| f1d48206-a19e-4616-9199-7349e367ff55 | worker-4090 | gen_video_ltx23_pose_by_url.json (LTX-2.5, request named the 2.3 checkpoint) | 140 s |
| 405a2bef-2040-4526-a2e0-a18749d4db23 | worker-4090 | LTX 2.3 comparison, same prompt and seed | – |
| 5438a8db-14f2-4c50-9d09-0615bd153cdc | worker-4090 | gen_video_minimax_h3_by_url.json | 115 s |
| 620f0ba5-804e-4975-9646-349f4201429b | worker-4090 | gen_animation_ltx25_by_url.json | 85 s |
| 10c0c506-a313-40bd-aa6a-ca0861243e0a | f12 | gen_animation_ltx25_hq_by_url.json | 210 s |

Quality (contact sheets): LTX-2.5 follows the motion part of the prompt more
fully than 2.3 (the push-in and the smile both happen); face identity drifts
in both. H3 holds the face identity clearly better than either LTX.

## 5. Deletion candidates (NOT deleted — owner decides)

Everything here can be re-downloaded from the URL shown and checked against
the sha256 (source hash; the box copies were not re-hashed).

| File | Size | sha256 (source) | Source | Boxes |
|---|---|---|---|---|
| diffusion_models/ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors | 25226571988 | 0a1d7aac2b338e8ec7e832149f1dcf11c9323272482b1cca0673d229702370f0 | https://huggingface.co/Kijai/LTX2.3_comfy | f5, f15, Raptor, worker-4090 (R:) |
| text_encoders/ltx-2.3_text_projection_bf16.safetensors | 2312149072 | 911d59bb4cb7708179c9a0045ea0fe41212ecfb77aed3a02702b7c0a8274911f | Kijai/LTX2.3_comfy | f5, f15, Raptor, worker-4090 |
| vae/LTX23_video_vae_bf16.safetensors | 1452258578 | 01ea62d09bc139f95c5dee7b5c062ad6a3e6cd8be910a1983ac02e7eb5b8ee3b | Kijai/LTX2.3_comfy | f5, f15, Raptor, worker-4090 |
| vae/LTX23_audio_vae_bf16.safetensors | 364855188 | 5bc10fa4adecf99dda132d916e23048cbd56797702c5fa50eb5d2079048a38c3 | Kijai/LTX2.3_comfy | f5, f15, Raptor, worker-4090 |
| checkpoints/ltx-2.3-22b-distilled-fp8.safetensors | 29531884062 | d9646b6f2d5c42d337b23671634c43bfeece6989644f51b4a3aa088465ccd3b2 | https://huggingface.co/Lightricks/LTX-2.3-fp8 | f5, f15, Raptor |
| checkpoints/ltx10eros_v14_2989669.safetensors | 29161843630 | – (Civitai, version 3109610) | https://civitai.com/models/2447875?modelVersionId=3109610 | f5, f15, Raptor, worker-4090 |
| checkpoints/ltx-2-19b-distilled-fp8.safetensors | 27078716346 | 8ae14327130c6ffdc87705b02c8e7654aa5c6d9a7f28a52d0acc1c30cb0d2932 | https://huggingface.co/Lightricks/LTX-2 | f5, f15, Raptor, f12 |
| loras/ltx-2-19b-lora-camera-control-static.safetensors (19B-only; LoRA agent's call) | 2214978664 | 6b79aad7ecdd60aef07f39177d1ac225a6608806086af94848436fec432e2d0d | https://huggingface.co/Lightricks/LTX-2-19b-LoRA-Camera-Control-Static | f5, f15, Raptor, f12, worker-4090 |
| loras/ltx-2-19b-distilled-lora-384.safetensors | 7674558424 | 2718f89582003cbb5b616635f18c091641917a3f3e5a2f2ad0fb3d5fdd153534 | Lightricks/LTX-2 | f15 |
| latent_upscale_models/ltx-2-spatial-upscaler-x2-1.0.safetensors | 995765578 | 3160fabf8edf0bc4dd8de40353a180813b111ce586b655ad54af9a7b8c6736de | Lightricks/LTX-2 | f15 |
| checkpoints/ltxv-13b-0.9.8-distilled-fp8.safetensors | 15694280140 | 111a3d07baa17f520e98b571e7916139ae0865c9a24b7534529d6b9e74264db3 | https://huggingface.co/Lightricks/LTX-Video | f5, f15, Raptor, f12 |
| checkpoints/ltxv-13b-0.9.8-dev-fp8.safetensors | 15694279916 | b281bbb53b76d25a02285c148212b32daa6a57dfa46ce804c9bddba46f948c94 | Lightricks/LTX-Video | f5, f15 |
| checkpoints/ltxv-13b-0.9.8-distilled.safetensors | 28579183564 | 2c5f814744f04d8118e0b5bbfc0742655b51e41a6c090da66fde2936d651a9c9 | Lightricks/LTX-Video | f5, f15 |
| text_encoders/gemma_3_12B_it_fp4_mixed.safetensors (and the duplicate in models/LLM) | 9447702218 | aaca463d11e6d8d2a4bdb0d6299214c15ef78a3f73e0ef8113d5a9d0219b3f6d | https://huggingface.co/Comfy-Org/ltx-2 (split_files/text_encoders) | f5, f15, Raptor, worker-4090 |
| text_encoders/gemma_3_12B_it.safetensors | 24379468890 | 56eaa964a0d9325d2dc9ecaf7759bfaf0fac78ae36c789bed6e03e275a3729ec | Comfy-Org/ltx-2 | f15 |

Kept on purpose: `clip/t5xxl_fp8_e4m3fn.safetensors` (shared with FLUX.1 —
image agent's decision), `ltx-2.3-22b-ic-lora-union-control-ref0.5` (used by
the LTX-2.5 control graphs), all LoRAs (LoRA agent owns them). LoRAs that lose
their base with the migration: `ltx-2-19b-lora-camera-control-static`,
`DreamLTXV` (LTXV 0.9.x, already marked obsolete). The LTX 2.3 style LoRAs
(Crisp Enhance, Amateur Hour, Pixar Toon, Dual-Character, EditAnything,
mvmt_lora_v2) are now applied on LTX-2.5 and need a visual re-check.
Wan-Animate-2 (worker-4090) is a deletion candidate once LTX-2.5 pose control
(or H3) is accepted as its replacement.
