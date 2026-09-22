# FLUX.1 removal from the image farm - 2026-09-23

FLUX.1 (Schnell, Dev, Fill-dev, the Dev Union ControlNet and the Flux.1-D LoRAs)
was replaced by **Z-Image Turbo** (fast tier, every image box) and **Krea 2 Turbo**
(quality tier, default image model) and then deleted from the fleet. This file is
the record written before deletion: every file, where it was, its size, its
SHA-256 and where it came from, so any of it can be restored byte-for-byte.

## What replaced what

| Old (FLUX.1) | New | Template (name kept) |
|---|---|---|
| Schnell text-to-image + aidmaMJ6.1 LoRA + 4x ESRGAN + RMBG | Z-Image Turbo fp8, 8 steps, res_multistep | `gen_image.json` |
| (none) | Krea 2 Turbo fp8, 8 steps, the image-service default | `gen_image_krea2.json` (new token) |
| Schnell + Dev Union-Pro 2.0 openpose + tiled Schnell refine + RMBG | Z-Image + Fun ControlNet Union 2.1 (2602) pose + RMBG | `t_pose.json` |
| Schnell + Dev Union-Pro pose/depth/canny | Z-Image + Fun ControlNet Union 2.1 | `gen_image_control_{pose,depth,canny}.json` |
| Schnell + Union-Pro (legacy modes) | Z-Image + Union 2.1 | `open_pose.json`, `gen_image_by_z_depth.json` |
| FLUX.1 Fill-dev (the mode was blocked, never usable) | Z-Image + Union 2.1 inpaint inputs | `inpaint.json` |
| Schnell img2img, tiled and plain | Z-Image img2img (5 steps, dpmpp_2m_sde, beta), TiledDiffusion kept | `detail_tiled.json`, `detail_plain.json`, `upscale_refine.json` |
| Schnell masked face repair | Z-Image masked face repair | `face_fix.json`, `face_fix_skin.json` |
| Schnell fp8 checkpoint on worker-4090 | dropped: worker-4090 advertises Krea 2 and Z-Image pose/depth | `gen_image_flux1_schnell.json` retired |

Encoders and VAE: Z-Image loads `qwen_3_4b.safetensors` and **the FLUX VAE
`vae/ae.safetensors`**, so that VAE stays. `t5xxl_fp8_e4m3fn.safetensors` stays: the
LTXV 13B history graph still names it. `clip_l.safetensors` and `t5xxl_fp16.safetensors`
were used only by FLUX.1 graphs (checked by grepping every template under
`backend/renderfin/assets/workflows`, the catalogue and the backend; the `clip_l`
hits in the SDXL graphs are `stop_at_clip_layer`, not the file).

## Validation before deletion (production)

- Z-Image text-to-image via `/api/image`: `c35474c7-e022-436d-8489-02478e1e2426` (Raptor, 12.3 s).
- Krea 2 as the image default via `/api/image`: `8c535342-c84c-442f-95b9-1556e482c71a` (Raptor, 43.3 s cold).
- Full character chain (Telegram-style, `autorig-bot`): job `5c35c168-a865-4004-983d-9c80b423f114`,
  T-pose variant A `2a558b2f` (Raptor, 35 s) and B `208a2b91` (f15), Hunyuan3D GLB on f12
  (quality report ok), turntable, convert/rig task `ffb2d754-9a89-4296-946e-6699c3163d7e` done,
  17/17 deliverables.
- Enhance via the public APIs: facefix `b031d29e` (Raptor, 32 s), detail `5d220f90`
  (Raptor, 28 s), upscale refine `1e4ff292` (Raptor).
- Real user traffic after the cutover (2026-09-22 20:16Z) on f5, f15 and Raptor finished
  without a failure.

## Inventory (deleted)

| Box | Path | Size | SHA-256 | Source |
|---|---|---:|---|---|
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\clip_l.safetensors` | 0.25 GB | `660c6f5b1abae9dc498ac2d21e1347d2abdb0cf6c0c0c8576cd796491d9a6cdd` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/clip_l.safetensors |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp16.safetensors` | 9.79 GB | `6e480b09fae049a72d2a8c5fbccb8d3e92febeb233bbe9dfe7256958a9167635` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/t5xxl_fp16.safetensors |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8.safetensors` | 2.14 GB | `393fc2a298b93ffe39f2db3f0d2ce11dfba62d44b7aa3c1dd3380d4a1be04deb` | fp8 conversion of https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0.safetensors` | 4.28 GB | `9d03f63f36206bab2f36aed5cfedc8693c2881397534e9d5f9ae9a0a41362517` | https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\FLUX.1-Fill-dev_fp8.safetensors` | 11.90 GB | `d1c6d962f6e81134e74da88b9b276c727db1c06f1e7b631b8aca3b7eff65e0fa` | fp8 conversion of https://huggingface.co/black-forest-labs/FLUX.1-Fill-dev |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\Detailer2K.safetensors` | 0.07 GB | `e075e511aa25073568d4a38ee7036946ef56102c9ba3fac85c35fbb85afa9bd0` | https://civitai.com/models/636355?modelVersionId=725298 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\FluxMythSharpL1nes.safetensors` | 0.08 GB | `6249169cf8b673b5170d4277a73dbf9c23f47581c79d65eb87a379e7756f911d` | https://civitai.com/models/599757?modelVersionId=2228091 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\NSFW_master.safetensors` | 0.17 GB | `84ce473cdeaa1ddd016cdf0406e08b43bb14c01d2c94ef48da2a035bb0b925c2` | https://civitai.com/models/667086?modelVersionId=746602 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\Sydney_Sweeney.safetensors` | 0.31 GB | `0e2caec1f87e429437d4057e2c92cee2d2898055dd36a8171ac15b2800a1dc82` | removed from Civitai; header modelspec.implementation = black-forest-labs/flux |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\UltraRealPhoto.safetensors` | 2.14 GB | `b1c4ddf95671e6b51817b4f3802865e544040c232c467e76b1cb0c251bd6b634` | https://civitai.com/models/796382?modelVersionId=1026423 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\aidmaMJ6.1-FLUX-v0.5.safetensors` | 0.08 GB | `ecb64944ab1ccbe62a7bcf1d149d9add41de9392d236542587a9cf326fdeb5a4` | https://civitai.com/models/646411?modelVersionId=1249246 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\flux_realism_lora.safetensors` | 0.02 GB | `379e73dccfb57822ee3b12f374e141ce1c79a13b7ff19da4219ef2a2a610038e` | https://civitai.com/models/631986?modelVersionId=706528 (XLabs, Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\microbikiniv12_FLUX.safetensors` | 0.15 GB | `f11f547ced215e152680cdba143d6553b178e910679469cf9c0a783d48e899a2` | https://civitai.com/models/122200?modelVersionId=1857758 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\skin texture style v5.safetensors` | 0.67 GB | `8e429dc8232ea00d988d9502a7e9a68d4c6e2fa7405a8e02ac0976214d2e23b4` | https://civitai.com/models/580857?modelVersionId=1081450 (Flux.1 D) |
| Raptor | `D:\ComfyUI_windows_portable\ComfyUI\models\unet\flux1-schnell.safetensors` | 23.78 GB | `9403429e0052277ac2a87ad800adece5481eecefd9ed334e1f348723621d2a0a` | https://huggingface.co/black-forest-labs/FLUX.1-schnell/blob/main/flux1-schnell.safetensors |
| Raptor | `X:\FleetModels\loras\flux\Detailer2K.safetensors` | 0.07 GB | `e075e511aa25073568d4a38ee7036946ef56102c9ba3fac85c35fbb85afa9bd0` | https://civitai.com/models/636355?modelVersionId=725298 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\Eye_Detail_Flux_Lora_-_Inpainting.safetensors` | 0.02 GB | `3107d1ffa5d185168fa33af4386dfb2b6ba813968e72f3c6de16ac7a7fb6bf7a` | https://civitai.com/models/263104?modelVersionId=1001942 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\FluxMythSharpL1nes.safetensors` | 0.08 GB | `6249169cf8b673b5170d4277a73dbf9c23f47581c79d65eb87a379e7756f911d` | https://civitai.com/models/599757?modelVersionId=2228091 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\NSFW_master.safetensors` | 0.17 GB | `84ce473cdeaa1ddd016cdf0406e08b43bb14c01d2c94ef48da2a035bb0b925c2` | https://civitai.com/models/667086?modelVersionId=746602 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\Sydney_Sweeney.safetensors` | 0.31 GB | `0e2caec1f87e429437d4057e2c92cee2d2898055dd36a8171ac15b2800a1dc82` | removed from Civitai; header modelspec.implementation = black-forest-labs/flux |
| Raptor | `X:\FleetModels\loras\flux\UltraRealPhoto.safetensors` | 2.14 GB | `b1c4ddf95671e6b51817b4f3802865e544040c232c467e76b1cb0c251bd6b634` | https://civitai.com/models/796382?modelVersionId=1026423 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\aidmaMJ6.1-FLUX-v0.5.safetensors` | 0.08 GB | `ecb64944ab1ccbe62a7bcf1d149d9add41de9392d236542587a9cf326fdeb5a4` | https://civitai.com/models/646411?modelVersionId=1249246 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\flux_realism_lora.safetensors` | 0.02 GB | `379e73dccfb57822ee3b12f374e141ce1c79a13b7ff19da4219ef2a2a610038e` | https://civitai.com/models/631986?modelVersionId=706528 (XLabs, Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\microbikiniv12_FLUX.safetensors` | 0.15 GB | `f11f547ced215e152680cdba143d6553b178e910679469cf9c0a783d48e899a2` | https://civitai.com/models/122200?modelVersionId=1857758 (Flux.1 D) |
| Raptor | `X:\FleetModels\loras\flux\skin texture style v5.safetensors` | 0.67 GB | `8e429dc8232ea00d988d9502a7e9a68d4c6e2fa7405a8e02ac0976214d2e23b4` | https://civitai.com/models/580857?modelVersionId=1081450 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\clip_l.safetensors` | 0.25 GB | `660c6f5b1abae9dc498ac2d21e1347d2abdb0cf6c0c0c8576cd796491d9a6cdd` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/clip_l.safetensors |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp16.safetensors` | 9.79 GB | `6e480b09fae049a72d2a8c5fbccb8d3e92febeb233bbe9dfe7256958a9167635` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/t5xxl_fp16.safetensors |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8.safetensors` | 2.14 GB | `393fc2a298b93ffe39f2db3f0d2ce11dfba62d44b7aa3c1dd3380d4a1be04deb` | fp8 conversion of https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0.safetensors` | 4.28 GB | `9d03f63f36206bab2f36aed5cfedc8693c2881397534e9d5f9ae9a0a41362517` | https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\FLUX.1-Fill-dev_fp8.safetensors` | 11.90 GB | `d1c6d962f6e81134e74da88b9b276c727db1c06f1e7b631b8aca3b7eff65e0fa` | fp8 conversion of https://huggingface.co/black-forest-labs/FLUX.1-Fill-dev |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\FLUX1\flux1-dev-fp8.safetensors` | 11.90 GB | `1be961341be8f5307ef26c787199f80bf4e0de3c1c0b4617095aa6ee5550dfce` | fp8 conversion of https://huggingface.co/black-forest-labs/FLUX.1-dev |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\FluxMythSharpL1nes.safetensors` | 0.08 GB | `6249169cf8b673b5170d4277a73dbf9c23f47581c79d65eb87a379e7756f911d` | https://civitai.com/models/599757?modelVersionId=2228091 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\NSFW_master.safetensors` | 0.17 GB | `84ce473cdeaa1ddd016cdf0406e08b43bb14c01d2c94ef48da2a035bb0b925c2` | https://civitai.com/models/667086?modelVersionId=746602 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\aidmaMJ6.1-FLUX-v0.5.safetensors` | 0.08 GB | `ecb64944ab1ccbe62a7bcf1d149d9add41de9392d236542587a9cf326fdeb5a4` | https://civitai.com/models/646411?modelVersionId=1249246 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\flux_realism_lora.safetensors` | 0.02 GB | `379e73dccfb57822ee3b12f374e141ce1c79a13b7ff19da4219ef2a2a610038e` | https://civitai.com/models/631986?modelVersionId=706528 (XLabs, Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\microbikiniv12_FLUX.safetensors` | 0.15 GB | `f11f547ced215e152680cdba143d6553b178e910679469cf9c0a783d48e899a2` | https://civitai.com/models/122200?modelVersionId=1857758 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\skin texture style v5.safetensors` | 0.67 GB | `8e429dc8232ea00d988d9502a7e9a68d4c6e2fa7405a8e02ac0976214d2e23b4` | https://civitai.com/models/580857?modelVersionId=1081450 (Flux.1 D) |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\text_encoders\clip_l.safetensors` | 0.25 GB | `660c6f5b1abae9dc498ac2d21e1347d2abdb0cf6c0c0c8576cd796491d9a6cdd` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/clip_l.safetensors |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\unet\flux1-schnell.safetensors` | 23.78 GB | `9403429e0052277ac2a87ad800adece5481eecefd9ed334e1f348723621d2a0a` | https://huggingface.co/black-forest-labs/FLUX.1-schnell/blob/main/flux1-schnell.safetensors |
| f15 | `D:\ComfyUI_windows_portable\ComfyUI\models\vae\FLUX1\ae.safetensors` | 0.34 GB | `afc8e28272cd15db3919bacdb6918ce9c1ed22e96cb12c4d5ed0fba823529e38` | second copy of the FLUX.1 VAE in vae\FLUX1 (the vae\ae.safetensors copy stays: Z-Image uses it) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\clip_l.safetensors` | 0.25 GB | `660c6f5b1abae9dc498ac2d21e1347d2abdb0cf6c0c0c8576cd796491d9a6cdd` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/clip_l.safetensors |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\clip\t5xxl_fp16.safetensors` | 9.79 GB | `6e480b09fae049a72d2a8c5fbccb8d3e92febeb233bbe9dfe7256958a9167635` | https://huggingface.co/comfyanonymous/flux_text_encoders/blob/main/t5xxl_fp16.safetensors |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8.safetensors` | 2.14 GB | `393fc2a298b93ffe39f2db3f0d2ce11dfba62d44b7aa3c1dd3380d4a1be04deb` | fp8 conversion of https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\controlnet\FLUX.1-dev-ControlNet-Union-Pro-2.0.safetensors` | 4.28 GB | `9d03f63f36206bab2f36aed5cfedc8693c2881397534e9d5f9ae9a0a41362517` | https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0 |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\diffusion_models\FLUX.1-Fill-dev_fp8.safetensors` | 11.90 GB | `d1c6d962f6e81134e74da88b9b276c727db1c06f1e7b631b8aca3b7eff65e0fa` | fp8 conversion of https://huggingface.co/black-forest-labs/FLUX.1-Fill-dev |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\FluxMythSharpL1nes.safetensors` | 0.08 GB | `6249169cf8b673b5170d4277a73dbf9c23f47581c79d65eb87a379e7756f911d` | https://civitai.com/models/599757?modelVersionId=2228091 (Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\NSFW_master.safetensors` | 0.17 GB | `84ce473cdeaa1ddd016cdf0406e08b43bb14c01d2c94ef48da2a035bb0b925c2` | https://civitai.com/models/667086?modelVersionId=746602 (Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\aidmaMJ6.1-FLUX-v0.5.safetensors` | 0.08 GB | `ecb64944ab1ccbe62a7bcf1d149d9add41de9392d236542587a9cf326fdeb5a4` | https://civitai.com/models/646411?modelVersionId=1249246 (Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\flux_realism_lora.safetensors` | 0.02 GB | `379e73dccfb57822ee3b12f374e141ce1c79a13b7ff19da4219ef2a2a610038e` | https://civitai.com/models/631986?modelVersionId=706528 (XLabs, Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\microbikiniv12_FLUX.safetensors` | 0.15 GB | `f11f547ced215e152680cdba143d6553b178e910679469cf9c0a783d48e899a2` | https://civitai.com/models/122200?modelVersionId=1857758 (Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\loras\skin texture style v5.safetensors` | 0.67 GB | `8e429dc8232ea00d988d9502a7e9a68d4c6e2fa7405a8e02ac0976214d2e23b4` | https://civitai.com/models/580857?modelVersionId=1081450 (Flux.1 D) |
| f5 | `D:\ComfyUI_windows_portable\ComfyUI\models\unet\flux1-schnell.safetensors` | 23.78 GB | `9403429e0052277ac2a87ad800adece5481eecefd9ed334e1f348723621d2a0a` | https://huggingface.co/black-forest-labs/FLUX.1-schnell/blob/main/flux1-schnell.safetensors |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\checkpoints\flux1-schnell-fp8.safetensors` | 17.24 GB | `ead426278b49030e9da5df862994f25ce94ab2ee4df38b556ddddb3db093bf72` | https://huggingface.co/AiAF/flux1-schnell-fp8.safetensors |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\loras\FluxMythSharpL1nes.safetensors` | 0.08 GB | `6249169cf8b673b5170d4277a73dbf9c23f47581c79d65eb87a379e7756f911d` | https://civitai.com/models/599757?modelVersionId=2228091 (Flux.1 D) |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\loras\NSFW_master.safetensors` | 0.17 GB | `84ce473cdeaa1ddd016cdf0406e08b43bb14c01d2c94ef48da2a035bb0b925c2` | https://civitai.com/models/667086?modelVersionId=746602 (Flux.1 D) |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\loras\flux_realism_lora.safetensors` | 0.02 GB | `379e73dccfb57822ee3b12f374e141ce1c79a13b7ff19da4219ef2a2a610038e` | https://civitai.com/models/631986?modelVersionId=706528 (XLabs, Flux.1 D) |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\loras\microbikiniv12_FLUX.safetensors` | 0.15 GB | `f11f547ced215e152680cdba143d6553b178e910679469cf9c0a783d48e899a2` | https://civitai.com/models/122200?modelVersionId=1857758 (Flux.1 D) |
| worker-4090 | `R:\ComfyUI_windows_portable\ComfyUI\models\loras\skin texture style v5.safetensors` | 0.67 GB | `8e429dc8232ea00d988d9502a7e9a68d4c6e2fa7405a8e02ac0976214d2e23b4` | https://civitai.com/models/580857?modelVersionId=1081450 (Flux.1 D) |

### f12 (pending: not reachable from the operator PC during this work)

f12 held, under `C:/AI/ComfyUI_windows_portable/ComfyUI/models`: `unet/flux1-schnell.safetensors`
(22.7 GB), `controlnet/FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8.safetensors` (2.0 GB),
`loras/aidmaMJ6.1-FLUX-v0.5.safetensors`, `clip/clip_l.safetensors` and
`clip/t5xxl_fp16.safetensors` (9.3 GB). Its image tokens were withdrawn from the renderfin
registry at the cutover (backup `f12.json.bak.zimage-*`), so no FLUX work reaches it.
Hashes and deletion follow once f12 has the Z-Image files.

## Deletion log

Deleted 2026-09-23 about 04:15 local farm time (21:15Z on 2026-09-22), one explicit path at a
time (`Remove-Item -LiteralPath`, no wildcards, no directory removal), each only after its
size matched the record above. Per-box log: `C:\ProgramData\AutoRig\zimage\flux1_delete.log`.

| Box | Files | Freed |
|---|---:|---:|
| f5 | 12 | 53.32 GB |
| f15 | 15 | 65.80 GB |
| Raptor (D: and the shared `X:\FleetModels\loras\flux`) | 25 | 59.56 GB |
| worker-4090 (`R:\ComfyUI_windows_portable`) | 6 | 18.33 GB |
| **Total** | **58** | **197.0 GB** |
| f12 | pending | about 36 GB |

Also retired at the same time:

- renderfin registry: f12 lost `gen_image.json` and the three `gen_image_control_*` tokens until
  it has the Z-Image files; worker-4090 lost `gen_image.json` and `gen_image_flux1_schnell.json`
  and its `gen_image.json -> gen_image_flux1_schnell.json` override (now an identity map).
- live catalogue: `flux1-schnell.safetensors`, `flux1-schnell-fp8.safetensors` and the five
  Flux.1-D LoRAs are `usable: false` with the reason shown to callers; FLUX.2 klein 4B stays
  and is no longer the image-service default (Krea 2 is).
- two stranded site requests that had picked "FLUX.1 Schnell FP8" (`7ab29aaa`, `7a68ee63`) were
  cancelled with that reason instead of waiting forever in Pending.
- the LoRA manager registry held no FLUX.1 LoRAs, so no sync agent will pull them back.

## Not deleted: candidates for the owner

Not FLUX.1, so left alone.

| Box | File | Size | Why it is a candidate |
|---|---|---:|---|
| worker-4090 | `checkpoints/CyberRealisticPony_V18.0_F16.safetensors` | 6.9 GB | SDXL/Pony; the owner wants realism and 3D styling, not anime |
| worker-4090 | `controlnet/xinsir-controlnet-union-sdxl-1.0.safetensors` | 2.5 GB | used only with Pony |
| Raptor | `X:/FleetModels/checkpoints/cyberrealisticPony_v180Coreshift_2764472.safetensors` | 6.9 GB | the same Pony checkpoint under its Civitai name |
| f15 | `diffusion_models/flux2_dev_fp8mixed.safetensors`, `flux-2-klein-base-9b-fp8.safetensors`, `text_encoders/mistral_3_small_flux2_fp8.safetensors` | 60 GB | FLUX.2 dev and klein 9B: not in the approved stack (klein 4B stays) |
| f15 | `diffusion_models/z_image_turbo_bf16.safetensors` | 12.3 GB | pre-existing bf16 copy; every template uses the fp8 file |
