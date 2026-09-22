# Video Avatar: reenactment, persistent identity and two-character story

Source review date: **2026-09-22**. The document began as an implementation plan. Sections explicitly marked **verified** below now record completed local canaries and the durable runtime promotion; unexecuted benchmark cases remain proposals.

## Decision

Use complementary backends rather than forcing one model to solve every problem. The ranking changed after measured canaries:

1. **Wan-Animate-2 + LightX2V six-step LCM** is the recommended single-avatar reenactment backend. It consumes the raw driving video directly and has now passed isolated 81-frame and 97-frame RTX 4090 canaries, including exact 960×540 delivery. Its durable ComfyUI 0.37 workflow is promoted on worker-4090. This is evidence for the tested seated speaking/hand-motion case, not for the complete benchmark or a two-avatar story.
2. **LTX-2.3 distilled + Union-Control IC-LoRA** is the first benchmark for pose/depth/canny controlled restaging, audio-video generation, and shots that must retain composition. The official Union adapter combines Canny, depth and pose; its 654 MB file uses a reference at half output resolution and the official ComfyUI workflow is published ([model card](https://huggingface.co/Lightricks/LTX-2.3-22b-IC-LoRA-Union-Control), [workflow JSON](https://github.com/Lightricks/ComfyUI-LTXVideo/blob/master/example_workflows/2.3/LTX-2.3_ICLoRA_Union_Control_Distilled.json)).
3. **Wan2.2 Animate v1** remains provenance and a rejected/weak baseline. Its full 97-frame test kept Maya reasonably coherent but framed the face poorly for roughly the first two seconds and required DWPose preprocessing. It is no longer the first choice.
4. Persistent identity remains a separate **Avatar Profile** containing canonical face/full-body/outfit references and model-specific adapters. Neither driving pose nor seed is identity. For images, use the previously proposed FLUX.2 klein 4B multi-reference path; for video, Wan-Animate-2 receives the avatar reference and raw driving video, while LTX receives an approved identity keyframe plus control video. Store results back against the same immutable avatar revision.

Wan2.2 Animate v1 must not be described as guaranteed identity preservation. Public users report plastic faces, weak resemblance, failures around props, long hair and body motion, and color/brightness shifts across extensions ([face/prop discussion](https://www.reddit.com/r/comfyui/comments/1nv0rb6/wan22_animate_test_comfyui/), [face resemblance discussion](https://www.reddit.com/r/comfyui/comments/1owf6xr), [long-sequence shift discussion](https://www.reddit.com/r/comfyui/comments/1of2up1/wan_22_animate_character_replacement_in_comfyui/)). These v1 reports remain useful failure tests but are not evidence against the separately verified Wan-Animate-2 implementation.

## Hardware reality on RTX 4090 24 GB

| Route | Published weights | 4090 assessment | License | Rank |
|---|---:|---|---|---:|
| Wan-Animate-2 INT8 ConvRot + LightX2V | 23.88 GiB exact five-file package | **Verified on 4090:** 81f and 97f native workflows completed; 97f/960×540 peaked at 19,619 MiB VRAM and 56,897 MiB RAM. | Apache-2.0 | **1 for reenactment** |
| LTX-2.3 distilled 1.1 FP8 already in farm | local file about 25 GB plus encoder/VAEs; 8 distilled steps | Existing 4090 workflow is the viable baseline; Union IC-LoRA adds 654 MB, plus preprocessors. Keep 960×544 latent and 960×540 delivery initially. | LTX-2 Community License | 1 for structural control |
| Wan2.2 Animate v1 FP8 + DWPose | about 25.5 GiB provisioned package | Executed at 25f and 97f, but the 97f driver framing hid/clipped the face for about the first two seconds; weaker evidence and more preprocessing than v2. | Apache-2.0 | rejected/legacy baseline |

### LTX-2.5 is current, but not yet the control baseline

As of this review, the official LTX repository recommends **LTX-2.5**, not 2.3. Its split pack replaces Gemma 3 with an LTX-specific Gemma 4 12B encoder and separates the distilled/dev transformer, video/audio VAEs, duration head and upscalers. The current BF16 component set is roughly 66–71 GB on disk depending on selected VAE/upscalers; it is not a one-file drop-in replacement for the installed 2.3 bundle ([official model list](https://github.com/Lightricks/LTX-2), [official 2.5 model card](https://huggingface.co/Lightricks/LTX-2.5/blob/main/README.md)). Google's vanilla Gemma 4 is not interchangeable with the fine-tuned LTX encoder.

LTX-2.5 brings the newer DFR production-quality pipeline and current FP8/offload support. Official CLI supports `--quantization fp8-cast` and `--offload cpu|disk`; CPU offload streams layers from pinned system RAM, disk offload is slower ([installation guide](https://github.com/Lightricks/LTX-2/blob/main/packages/ltx-pipelines/docs/installation.md), [optimization guide](https://github.com/Lightricks/LTX-2/blob/main/packages/ltx-pipelines/docs/optimization.md)). This makes a 4090 experiment plausible, but no official source reviewed here establishes a comfortable 24 GB bound for the complete 2.5 DFR stack. Measure it before promotion.

The decisive blocker for this task is adapter availability: the published **pose + depth + canny Union-Control is explicitly trained on LTX-2.3-22B**, while current 2.5 IC-LoRAs found in the official collection target narrower effects such as decompression or water simulation. Model-family adapters cannot be assumed portable. Therefore:

- keep 2.3 distilled FP8 + 2.3 Union-Control as the executable structural-control benchmark;
- add a separate 2.5 distilled FP8-cast/offload image-to-video/DFR quality benchmark when disk/RAM capacity permits;
- do not replace the working 2.3 control route until Lightricks publishes a 2.5 Union-Control adapter or a locally trained 2.5 adapter passes the same eight cases;
- never silently load the 2.3 Union adapter into 2.5.

This satisfies the requirement to maintain modern versions without deleting the only current compatible control path. A newer base is a candidate upgrade, not evidence that its missing controls already work.

Concrete 4090 candidate audit on 2026-09-22 found an official Comfy int8 pack of about **37.0 GiB** on disk: distilled transformer 20.03 GiB (`c4279eef…44334`), LTX-specific Gemma4 encoder 14.32 GiB (`6ce688a0…1487f`), video DiffVAE 1.37 GiB (`847e14ca…dba3`), audio VAE 0.34 GiB (`c52733d3…d37a5`) and spatial upscaler 0.93 GiB (`eb5a71fe…677e8`). The lighter convolutional video VAE is 1.35 GiB (`685b06ee…fce8d`). These identities come from the official [LTX-2.5 repository](https://huggingface.co/Lightricks/LTX-2.5); it is `gated=auto`, so accepting its access terms through an authorized Hugging Face account is required before download.

At the time of the initial audit, the 4090 runtime was ComfyUI 0.21.1 and lacked `LTXVDualCFGGuider`, so the gated 37 GiB LTX-2.5 pack was not downloaded. That runtime blocker has since been removed: the durable worker is now ComfyUI **0.37.0**, pinned to commit `e638023d54497dbe0579565e5de4bb7076899592`, and passed object-info compatibility for all advertised workflows. LTX-2.5 still remains a separate future candidate because its gated weights were not admitted and its pose/depth/canny replacement for the proven 2.3 Union path was not validated. Do not patch a 2.5 graph to use an older adapter and call it equivalent.

The initial Wan2.2 Animate v1 audit found that the upstream BF16 route targeted much larger hardware, which is why the earlier FP8/DWPose path required local measurement ([official repo](https://github.com/Wan-Video/Wan2.2)). That historical concern is now superseded for the recommended backend by the measured Wan-Animate-2 INT8 ConvRot results above.

LTX-2.3 Union-Control is under the LTX-2 Community License, not Apache. The license contains special commercial-entity terms and may require a Commercial Use Agreement depending on the entity/use; legal/product review must record the applicable grant before a paid service enables it ([license text](https://huggingface.co/Lightricks/LTX-2.3-22b-IC-LoRA-Union-Control/blob/main/LICENSE)).

## Exact executable workflows

### A. Wan-Animate-2 — verified workflow

Primary sources:

- Upstream implementation: <https://github.com/Wan-Video/Wan-Animate-2>
- Official Comfy package: <https://huggingface.co/Comfy-Org/Wan-Animate-2>
- Official Comfy workflow: <https://github.com/Comfy-Org/workflow_templates/blob/main/templates/video_wan_animate2.json>

The exact promoted contract is the official Comfy base-plus-LoRA path, not the upstream standalone distilled settings:

```text
DiT: wan_animate_2_int8_convrot.safetensors
LoRA: lightx2v_I2V_14B_480p_cfg_step_distill_rank64_bf16.safetensors
text: umt5_xxl_fp8_e4m3fn_scaled.safetensors
vision: clip_vision_h.safetensors
VAE: Wan2_1_VAE_bf16.safetensors
sampler/scheduler: LCM / simple
steps: 6
conditioning: WanAnimate2ToVideo(reference image + raw driving video)
cache: WanAnimate2Cache on CPU/int8
```

The five-file package totals 25,645,113,843 bytes (23.88 GiB). Exact SHA-256 receipts and the isolated-runtime history are preserved in `wan-animate2-candidate-20260922.md`. Wan-Animate-2 needs no DWPose intermediates; it consumes raw driver frames.

The durable runtime is `R:\autorig\.runtime\onlyrender`, ComfyUI 0.37.0 at commit `e638023d54497dbe0579565e5de4bb7076899592`. Promotion occurred only after CPU object-info validation and sequential GPU regressions for Pony, FLUX.2 and LTX2.3. Worker-4090 now advertises `gen_video_wan_animate2_by_url.json`; the old 0.21.1 controller remains the rollback path.

Measured acceptance:

- 81f, 832×480, 24 fps: 249.43 s; peak 19,710 MiB GPU, 50,514 MiB system RAM.
- 97f, internal 960×544 and exact 960×540 delivery: 310.12 s; peak 19,619 MiB GPU, 56,897 MiB system RAM.
- The 97f run used exactly two static context windows, 0–20 and 5–25, and had no continuation seam or OOM.
- All 97 frames were reviewed; identity, clothing, background, face and two hands stayed coherent in the tested seated speaking/hand-motion clip.

This promotes the implementation path, not every creative use. Profile turns, prop exchange, fast dance and two-person separation still require the benchmark below.

### B. LTX-2.3 Union-Control

Use the official ComfyUI graph directly as the starting artifact:

<https://github.com/Lightricks/ComfyUI-LTXVideo/blob/master/example_workflows/2.3/LTX-2.3_ICLoRA_Union_Control_Distilled.json>

Required adapter:

```text
models/loras/ltx-2.3-22b-ic-lora-union-control-ref0.5.safetensors
```

The official graph uses `LTXICLoRALoaderModelOnly` to read the adapter's reference downscale factor and `LTXAddVideoICLoRAGuide` to inject the control video. The adapter is trained for Canny + depth + pose, with reference resolution 0.5× output ([model card](https://huggingface.co/Lightricks/LTX-2.3-22b-IC-LoRA-Union-Control)).

For precomputed depth, Lightricks confirms direct depth-video input is valid. Width and height must be divisible by 64 because the reference is halved and the LTX grid still requires multiples of 32; the provided graph resizes automatically ([official discussion](https://huggingface.co/Lightricks/LTX-2.3-22b-IC-LoRA-Union-Control/discussions/1)).

Start settings for the installed distilled 1.1 FP8 path:

```text
output latent: 960x544
delivery: 960x540
frames: 97 first, then 121 if stable
fps: 24/25 according to source clip, fixed throughout a case
CFG: 1.0
steps/sigmas: official distilled eight-step schedule
IC-LoRA reference factor: 0.5
control variants: pose-only, depth-only, canny-only, then union
```

Do not load old `LTX-2-19b-IC-LoRA-Pose/Depth/Canny-Control` or 0.9.7 13B adapters into LTX-2.3. They belong to different base families. The 2.3 Union-Control file is the compatible candidate.

## Benchmark: 8 reference reenactments

Use synthetic or properly licensed adult human clips, each 3–5 seconds, single continuous shot, clean 24/30 fps, no cuts. Each case runs Wan Animate and the relevant LTX control variants with the same avatar revision. The reference set deliberately escalates difficulty.

| # | Driving action | Why it exists | LTX channels | Pass conditions |
|---:|---|---|---|---|
| 1 | Frontal neutral talking head, blink and small head turn | identity/facial baseline | pose, canny | stable eyes/teeth/hairline; lip motion; no face swap or background pumping |
| 2 | Waist-up hand wave crossing the torso | hands + occlusion | pose, depth | five fingers when visible; arm returns correctly; torso/outfit does not melt |
| 3 | Full-body walk toward camera, then stop | scale/depth change | pose, depth | leg cadence, foot contact, consistent height/body build and face |
| 4 | Side profile turn to three-quarter view | unseen-angle identity | pose, canny | nose/jaw/ear remain compatible with avatar refs; no identity jump at profile |
| 5 | Sit on chair and cross legs | human-object contact | pose, depth, canny | chair topology and limb order remain correct; no fusion with seat |
| 6 | Lift a plain cup, sip, replace it | prop interaction stress | pose, depth, canny | cup count/shape stable; fingers do not fuse; mouth contact is plausible |
| 7 | Fast two-step dance with hair/clothing motion | temporal/high-motion stress | pose, depth | no framewise body duplication; hair follows motion without replacing face |
| 8 | Two adults exchange an object | multi-person identity leakage | pose, depth, canny | Avatar A/B remain distinct; object transfers once; limbs do not cross-identities |

For every case preserve:

- source video checksum, fps, frames and extracted pose/depth/canny checksums;
- avatar-profile revision, reference images and clothing revision;
- exact model/adapters SHA-256, workflow JSON hash, node versions and seed;
- peak VRAM/RAM, wall time and every output frame/video;
- an uninterrupted playback review. Still frames alone cannot pass a temporal benchmark.

Score face identity per detected frame, body/outfit similarity on segmented crops, pose error against the driver, temporal landmark jumps, hand/limb defect count, background stability and human side-by-side preference. Calibrate thresholds on local positive/negative avatar pairs. A face-recognition score is evidence, not the acceptance decision.

## Persistent Avatar Profile for image and video

Reuse the `CHARACTER_PROFILE` concept from the character-consistency research, specialized as `AVATAR_PROFILE`:

```json
{
  "schema": "autorig.avatar-profile/v1",
  "id": "avatar_a",
  "revision": 1,
  "canonical_refs": {
    "face": ["front", "left_3q", "right_3q", "profile"],
    "full_body": ["front_neutral", "side_neutral"],
    "outfit": ["outfit_default_front", "outfit_default_back"]
  },
  "adapters": {
    "image": {"backend": "flux2-klein-4b", "mode": "multi_reference"},
    "wan_animate2": {"mode": "raw_driving_video_plus_reference_image", "steps": 6},
    "wan_animate_v1": {"mode": "legacy_diagnostic_only"},
    "ltx23": {"mode": "approved_keyframe_plus_union_control", "reference_factor": 0.5}
  },
  "validation_receipts": []
}
```

Face, body, outfit and style remain separate. Generate an approved neutral keyframe and turnaround before video. Wan-Animate-2 uses the most angle-appropriate portrait/full-body reference with the raw driver. LTX uses an approved image keyframe of that avatar plus the chosen control map. Every output records the avatar revision; changing hairstyle/outfit creates a new wardrobe revision rather than silently replacing identity.

Seed is stored for render reproducibility but is not identity. A trained identity LoRA may later augment a profile, but it must be tied to the exact base model and cannot be reused across FLUX, Wan and LTX by filename alone.

## Two-avatar coherent story after single-avatar acceptance

Do not begin with a long two-person generation. Build a 20–30 second story from 4–6 short shots:

1. Establishing shot: A and B approach a table from separate sides.
2. Medium A: A places a small red box on the table.
3. Medium B: B reacts, reaches toward the box.
4. Two-shot: A passes the box to B.
5. Close B: B opens it and smiles.
6. Closing wide: both leave in opposite directions.

Use locked `avatar_a@revision` and `avatar_b@revision`, one immutable environment plate, fixed wardrobe and a story-state manifest (`box_owner`, positions, time of day, camera side). Generate/approve the first keyframe of every shot before animation. Use Wan-Animate-2 for close/medium single-character expression and body reenactment; use LTX pose/depth/canny for the establishing shot, object geography and the two-shot. Cut at natural action boundaries instead of extending one diffusion clip.

Continuity gates between shots:

- A and B face embeddings must match their own refs more closely than each other's;
- clothing colors, hair silhouette, height ratio and screen direction remain stable;
- the box exists once, stays red, and ownership changes only in shot 4;
- camera stays on the chosen 180-degree axis;
- first/last frames align at cuts, and full playback has no brightness/color jumps.

### Audit of the installed `LTX2.3-IC-LORA-Dual-Character`

The installed 327 MB file is authentic: SHA-256 `b6c3199e2c95eb0aad0cea4f3e8dfa33e8f6966d85bc79e3994faf5a57406103`, matching Civitai version 2810376 and the byte-identical mirror. Despite its filename, it is **not a parallel-canvas IC-LoRA and does not accept two independent identity references**. A field-tested mirror documents an A/B/C conditioning test and identifies the real mechanism as an ordinary LTX LoRA plus standard first-frame I2V pinning ([corrected model card](https://huggingface.co/SyFeee/LTX2.3-Dual-Character-en)).

Correct use is therefore:

1. Create one approved composite first frame containing Avatar A and Avatar B in their intended screen positions.
2. Run the normal LTX-2.3 distilled I2V workflow with this file as the frame-0 pin.
3. Apply `LTX2.3-IC-LORA-Dual-Character.safetensors` through the ordinary model LoRA loader at strength 0.8 initially (documented range 0.7–0.9; original author says 0.6–1.0 standalone).
4. Use 24 fps and a frame count satisfying `8k+1`; 960×544 is the 4090 preview bucket, while published examples use 1280×704×121.
5. Re-pin the composite first frame for every shot. Do not expect one long multi-shot render to preserve both identities.

Two separate refs cannot both be pinned at frame 0. A staggered second pin is possible only at a later VAE boundary and does not preserve B from the first frame. The composite first-frame pattern is the recommended route. No `LTXICLoRALoaderModelOnly` or `LTXAddVideoICLoRAGuide` belongs in this LoRA's workflow; presenting it as such would be incorrect.

Reported failure modes are highly relevant to acceptance: quoted dialogue can hallucinate burned-in subtitle text; dark clothing tends to lighten; complex contact/hugging can deform; detached props disappear; last 6–8 frames may smear; portrait orientation weakens identity. Use indirect narration, repeat critical garment colors, keep physical interaction modest, reject configurations with failed tails rather than using a shortened clip as proof of full-length quality, and render one shot per clip.

The original Civitai description simultaneously says Apache-2.0 and “educational/exchange use only.” That ambiguity must be resolved before commercial enablement even though mirrors label Apache-2.0. Keep the catalogue entry experimental and describe it as `dual-dialogue I2V LoRA (single composite first-frame pin)`, not as a two-reference identity adapter.

## Failure policy and ranking

1. **Use the promoted Wan-Animate-2 path for the remaining single-avatar cases 2–7.** Preserve per-run telemetry and full-frame review. The measured seated 97f result is the baseline to beat.
2. **Keep LTX-2.3 Union-Control for pose/depth/canny structure.** Run cases 1–7 channel-by-channel. Union may overconstrain textures; compare it against the strongest single channel.
3. **Keep Wan2.2 Animate v1 disabled as a public choice.** Its 97f result had weak full-clip face coverage and offers no measured advantage over Wan-Animate-2.
4. **Run case 8 and the short story only after both avatar profiles pass the remaining single-avatar tests.** Multi-person prompts can blend identities; use separate approved shot keyframes and explicit spatial masks/regions where supported.

Reject and retry a shot for: face identity crossing, missing/extra limbs, persistent hand-object fusion, garment mutation, uncontrolled camera cut, background replacement, duplicated prop/person, frozen motion, audio/video decode failure or a discontinuity visible at normal playback speed. Report partial success per case/channel; do not convert one attractive frame into a claim that reenactment works.

## Measured Wan2.2 Animate v1 result — retained as rejected baseline

After the research phase, one bounded 4090 canary ran with the provisioned FP8 workflow. It used a safe synthetic adult avatar portrait, the official Comfy driving-video fixture resampled to 25 frames at 24 fps, internal 832×480 generation, six steps, CFG 1, Euler/simple and seed 20260922. Both face-only and body+hands DWPose maps were generated automatically inside the graph.

The job completed in 95.6 seconds without node errors or OOM. Output decoded as H.264, 25 frames, 24 fps, 1.0417 seconds; a separate delivery copy decoded at 960×540 with the same timing. Inspection of all 25 frames found a stable one-person count, coherent face/hair/jacket, continuous arm motion and no missing/extra limbs. This proved that the old pipeline executed, but it is not the recommended backend after Wan-Animate-2 validation.

A meaningful failure was also visible: an unwanted tattoo-like forearm detail appeared even though it was absent from the Avatar reference. This proves appearance inconsistency. A similar feature existed on the driver, but this workflow passes pose/facial-landmark maps rather than driver skin RGB, so a causal transfer of that texture is not established. Hand details were plausible at contact-sheet scale but not flawless. The next benchmark must include plain clothing/skin in the driver, explicit tattoo/accessory mismatch tests, profile views and object interaction before the backend can be promoted beyond candidate status.

A second full 97-frame run tested whether stronger appearance constraints reduce that appearance error. Settings were 24 fps, 4.0417 seconds, internal 832×480, exact 960×540 delivery, six steps, CFG 1, Euler/simple and fixed seed 2026092201. The prompt fixed the mustard long-sleeved jacket to both wrists and explicitly required clean unmarked skin; tattoo/body-art and rolled-sleeve terms were negative.

The job completed successfully in 215.7 seconds without OOM or node errors. Every frame was reviewed in four chronological contact sheets. The jacket, bob haircut, face and one-person count remained coherent; both arms stayed covered and the unwanted tattoo did not appear. The complete arm sweep and hair-touch action was continuous. Hands remained anatomically plausible, including the hand moving close to camera, though motion blur prevents treating fine fingers as a high-detail hand benchmark.

Coverage was not uniformly identity-readable: the driver's pose places a sleeve across the frame, leaving the face partly or wholly outside roughly the first two seconds. This is a control-video composition limitation rather than proof of identity failure, but it makes the full-clip result too weak for promotion. Wan2.2 Animate v1 remains disabled/rejected as the first-choice backend.

## Current acceptance boundary

The durable ComfyUI 0.37 promotion and Wan-Animate-2 97-frame canary establish a working production-capable route for the tested single-avatar seated motion. They do **not** prove the planned two-avatar story, profile-view fidelity, prop exchange, fast dance, long-form continuation or cross-shot identity. Those remain open benchmark work and must not be described as complete.
