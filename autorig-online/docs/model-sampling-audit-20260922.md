# Model sampling audit — 2026-09-22

## Scope and status

This document audits the 18 entries currently declared in
`deploy/ai-models/model_catalogue.json`. It separates three things that must not
be conflated:

1. **Author recommendation** — prose or a first-party reference workflow.
2. **Example metadata** — settings embedded in images/videos published by the
   author. These are evidence of successful examples, not necessarily a stated
   universal preset.
3. **Product Auto preset** — the setting AutoRig chooses for a supported
   model/workflow. A product preset may deliberately differ from an author's
   minimum, but that difference must be identified and validated.

The policy described below is the target policy under review. It must not be
described as deployed until the corresponding API validation, UI locks, Auto
sentinels, catalogue data, and runtime behavior are updated and tested.

## Guidance, CFG, and embedded guidance

`CFG`, Diffusers `guidance_scale`, and a model's embedded/distilled guidance are
not interchangeable controls.

- FLUX.1 Schnell is guidance-distilled. The official Diffusers example uses
  `guidance_scale=0.0`. Our Comfy workflows use `BasicGuider`, which has no
  classifier-free positive/negative mixing control. A UI value displayed as
  CFG 1 is therefore a product convention for “no extra amplification,” not a
  literal implementation of Diffusers guidance 1.
- Distilled FLUX.2 Klein 4B uses the official four-step path with guidance 1.0.
  Our workflow again uses `BasicGuider`; arbitrary CFG values cannot be applied
  by that graph.
- LTX 2.3 and 10Eros workflows contain a real `CFGGuider` at CFG 1. Their
  trained manual sigma schedules are part of the model contract.
- CyberRealistic Pony uses a conventional SDXL `KSampler`; CFG 5 is a real CFG
  value.

An unsupported CFG or generic scheduler must be rejected early. It must not be
returned as an “effective” parameter when the selected native graph cannot
apply it. UI controls should be locked to the model contract. Work to enforce
these rules, and to distinguish a true Auto sentinel from numeric zero, is
pending until the root implementation is updated and verified.

## Checkpoint policy

| Catalogue entry | Source evidence | Audited workflow | Target Auto policy | Audit result |
|---|---|---|---|---|
| FLUX.2 Klein 4B distilled | [BFL model card](https://huggingface.co/black-forest-labs/FLUX.2-klein-4B), [BFL inference repository](https://github.com/black-forest-labs/flux2#distilled-vs-base), [official Comfy distilled edit template](https://github.com/Comfy-Org/workflow_templates/blob/main/templates/image_flux2_klein_image_edit_4b_distilled.json) | Text, reference edit, and avatar graphs use Euler, native `Flux2Scheduler`, four steps, and `BasicGuider`. | **Fixed 4 steps**, Euler, native Flux2 scheduler, product guidance display 1. | Correct. BFL distinguishes distilled four-step models from 50-step Base models. Raising distilled Klein above four is not an “upper quality” setting; explicit values above four should be rejected. Text/edit/avatar use the same sampling contract. |
| FLUX.1 Schnell BF16/farm UNET | [BFL model card](https://huggingface.co/black-forest-labs/FLUX.1-schnell), [BFL reference CLI](https://github.com/black-forest-labs/flux/blob/main/src/flux/cli.py), [official Comfy Flux examples](https://github.com/comfyanonymous/ComfyUI_examples/tree/master/flux) | `gen_image.json`: Euler, simple scheduler, four steps, `BasicGuider`. | **Auto 4, allowed range 1–4**, Euler/simple, no external CFG amplification. | Correct at Auto 4. BFL states Schnell generates in one to four steps and its reference defaults to four. More than four should be rejected rather than presented as higher quality. |
| FLUX.1 Schnell FP8 checkpoint | Same BFL sources; quantized file page: [AiAF FP8](https://huggingface.co/AiAF/flux1-schnell-fp8.safetensors/blob/main/AiAF/flux1-schnell-fp8.safetensors) | `gen_image_flux1_schnell.json`: Euler, simple, four, `BasicGuider`. | Same as BF16 Schnell. | Correct. Quantization changes memory/quality risk, not the trained sampling schedule. |
| LTX 2.3 distilled 1.1 | [Lightricks model card](https://huggingface.co/Lightricks/LTX-2.3), [official single-stage distilled workflow](https://github.com/Lightricks/ComfyUI-LTXVideo/blob/master/example_workflows/2.3/LTX-2.3_T2V_I2V_Single_Stage_Distilled_Full.json) | Eight-step manual sigma schedule, CFG 1, Euler ancestral CFG++. | **Fixed 8 steps**, CFG 1, `euler_ancestral_cfg_pp`; native/manual schedule locked. | Correct. Runtime must refuse a different step count rather than invent a new sigma schedule. |
| LTX 10Eros v1.4 | [author model/version](https://civitai.com/models/2447875?modelVersionId=3109610), [author V5 DMD workflow](https://huggingface.co/TenStrip/LTX2.3-10Eros_Workflows/blob/main/10Eros_10SNodes_I2V_Basic_DMD_V5.json) | Nine-step author sigma schedule, CFG 1, Euler ancestral. | **Fixed 9 steps**, CFG 1, Euler ancestral; manual schedule locked. | Correct after the full model/workflow validation. Example metadata also consistently shows nine steps, Euler A, CFG 1. |
| CyberRealistic Pony v18 CoreShift | [author model/version](https://civitai.com/models/443821?modelVersionId=2884631), [version API](https://civitai.com/api/v1/model-versions/2884631) | SDXL KSampler, DPM++ 2M SDE, Karras, CFG 5, Clip Skip 2. | **Product Auto 50 steps**, CFG 5, DPM++ 2M SDE/Karras, Clip Skip 2. UI maximum 60. | Author says 30+ steps and publishes no upper bound; all captured v18 examples use 30. Auto 50 is therefore a product quality preset requested toward the upper part of the UI range, not an author preset. It remains pending validation before being claimed as current behavior. |

For step-distilled models, “choose Auto near the upper recommended bound” means
four for Schnell/Klein, eight for distilled LTX 2.3, and nine for 10Eros. It
does not mean raising every model toward the global slider maximum.

## FLUX.1 Dev-trained LoRAs on Schnell

The five FLUX LoRAs below were trained for FLUX.1 Dev. The installed production
base is Schnell; the user explicitly does not want FLUX Dev downloaded. The
combination can load and has passed technical canaries, but it is a
compatibility fallback rather than the author-validated pairing.

Their catalogue field `sampling_recommendations_compatible: false` is correct:
author example steps/CFG/sampler/scheduler must be preserved as provenance but
must not override Schnell's Auto 4 contract. Only an author-supported LoRA
strength and trigger should be applied automatically.

| LoRA | Author/source evidence | Author/example settings | Product behavior with Schnell |
|---|---|---|---|
| NSFW MASTER v1.0 | [model/version](https://civitai.com/models/667086?modelVersionId=746602), [version API](https://civitai.com/api/v1/model-versions/746602) | Author weight 0.8; captured examples use 20 steps/Euler. No scheduler is present in the captured metadata, so `simple` must not be described as author-specified. | Strength 0.8; Schnell Auto 4/Euler/simple. Mark base mismatch. |
| XLabs Flux Realism, Comfy conversion | [model/version](https://civitai.com/models/631986?modelVersionId=706528), [version API](https://civitai.com/api/v1/model-versions/706528) | Dev-based conversion; captured evidence only establishes Euler, not a numeric step upper bound. | Keep a validated/default strength; Schnell Auto 4. Do not invent a Dev step count. |
| Realistic Skin Texture F1D v2.5 | [model/version](https://civitai.com/models/580857?modelVersionId=1081450), [version API](https://civitai.com/api/v1/model-versions/1081450) | Examples: 20 steps, CFG 1, Euler, Simple. Triggers: `skin texture style`, `realism`, `detailed`. | Preserve triggers; Schnell Auto 4. Example dimensions 1024×1024 do not override product 960×540. |
| Mythic Fantasy — Flux Sharp Lines | [model/version](https://civitai.com/models/599757?modelVersionId=2228091), [version API](https://civitai.com/api/v1/model-versions/2228091) | Author weight 1, range 0.5–1.5; examples: 30 steps, CFG 1, DPM++ 2M, SGM Uniform. Trigger `SharpL1nes`. | Strength 1 and trigger; Schnell Auto 4. Dev sampler/scheduler remain provenance only. |
| Micro Bikini v1.2 Flux | [model/version](https://civitai.com/models/122200?modelVersionId=1857758), [version API](https://civitai.com/api/v1/model-versions/1857758) | Author weight 0.7–1.0; product midpoint 0.8. Nine examples use 25 steps and one uses 30; all use CFG 1/Euler/Beta. Triggers `micro bikini`, `from behind`. | Strength 0.8 and triggers; Schnell Auto 4. Dev example sampling remains suppressed. |

## FLUX ControlNet compatibility fallback

The current pose/depth/canny graphs load Schnell at four-step Euler/simple but
use `FLUX.1-dev-ControlNet-Union-Pro-2.0-fp8`. The [author model card](https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0)
explicitly identifies FLUX.1 Dev as its base. Passing a canary proves technical
execution, not author validation on Schnell. Raising steps or CFG does not
repair the base-model mismatch.

The [author demo](https://huggingface.co/spaces/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro-2.0/blob/main/app.py)
uses these mode presets:

| Mode | Strength | Guidance end |
|---|---:|---:|
| Canny | 0.70 | 0.80 |
| Depth | 0.80 | 0.80 |
| OpenPose | 0.90 | 0.65 |

The current generic 0.80/end 1.00 is not author-correct: it overextends every
mode, is too strong for canny, and is too weak for pose. Until a compatible
commercial base/control route is selected, label these workflows as Schnell
compatibility fallbacks. Do not download FLUX Dev as part of this policy.

## LTX LoRA policy

All LTX 2.3 LoRAs inherit the selected compatible base workflow's fixed
eight-step schedule unless the author explicitly requires a different,
non-distilled workflow. A LoRA must never silently replace a trained manual
sigma schedule.

| LoRA | Author/source evidence | Target behavior |
|---|---|---|
| LTX 2.3 Crisp Enhance | [model/version](https://civitai.com/models/2535622?modelVersionId=2849716) | No numeric author settings. Inherit fixed LTX 2.3 Auto 8. |
| Amateur Hour rank16 | [model/version](https://civitai.com/models/2530917?modelVersionId=2844417) | No numeric author settings. Inherit fixed Auto 8. |
| IC-LoRA Dual Character | [model/version](https://civitai.com/models/2500098?modelVersionId=2810376) | Author strength range 0.6–1.0; product midpoint 0.8. Sampling remains fixed Auto 8. |
| EditAnything | [model/version](https://civitai.com/models/2553102?modelVersionId=2869279) | Example CFG 1 and author instruction triggers. Sampling remains fixed Auto 8. This LoRA still requires its compatible edit-conditioning graph; a file being present is insufficient proof. |
| Better Human Motion v2 LTX23 | [model/version](https://civitai.com/models/2734359?modelVersionId=3087364) | Author weight 0.4–0.8 and 15–30 steps; product midpoint strength 0.6. The 15–30 range belongs to a separate non-distilled path and is suppressed on the fixed distilled workflow. Inherit Auto 8; do not expose 30 as effective until that separate workflow is validated. |
| Pixar CGI Toon | [model/version](https://civitai.com/models/2536130?modelVersionId=2850271) | Trigger `P1x4r` is required. No numeric sampling or strength recommendation is published. Inherit Auto 8. |
| DreamLTXV | [model/version](https://civitai.com/models/1264762?modelVersionId=1426312) | Legacy LTXV base, author weight range 0.5–1.0; product midpoint 0.75. Keep its legacy `gen_animation_by_url` schedule. Do not silently route it through LTX 2.3 Auto 8. |

## Required implementation gates

These items are policy conclusions, not deployment claims:

- Auto must be a true sentinel distinct from numeric zero.
- Schnell and distilled Klein must lock Auto/max steps to 4.
- Distilled LTX 2.3 must lock steps to 8; 10Eros must lock to 9.
- Pony Auto 50 is a product preset pending validation; the UI may expose up to
  60, while clearly showing the author's documented baseline is 30+.
- Unsupported CFG and scheduler controls must fail validation before enqueue.
  They must not be echoed as effective while being ignored by the graph.
- Native schedulers (`Flux2Scheduler`, trained manual sigmas) must remain
  native. A generic scheduler dropdown must not overwrite them.
- Example dimensions never override the product default 960×540.
- Dev-trained FLUX LoRAs and the Dev ControlNet running on Schnell must be
  labeled compatibility fallbacks. No FLUX Dev download is part of this plan.
- Every new model/LoRA route still requires worker file eligibility and a real
  canary of the exact workflow before the UI calls it supported.
