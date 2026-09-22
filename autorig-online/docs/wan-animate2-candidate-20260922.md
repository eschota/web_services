# Wan-Animate-2 candidate audit — 2026-09-22

## Decision status

Wan-Animate-2 is a current-generation candidate for character animation. The
official model package is downloaded and hash-verified on the local 4090
machine, but it is **not enabled in production**. One isolated GPU canary has
completed successfully; the live ComfyUI checkout was not upgraded, and the
stock worker was restored afterward.

The existing Wan Animate generation and its weights remain intact. This
candidate is additive and must be validated in an isolated runtime before any
cutover.

## Primary sources

- Upstream implementation and release notes:
  https://github.com/Wan-Video/Wan-Animate-2
- Official Comfy-repackaged weights:
  https://huggingface.co/Comfy-Org/Wan-Animate-2
- Official Comfy workflow:
  https://github.com/Comfy-Org/workflow_templates/blob/main/templates/video_wan_animate2.json
- Native Comfy model implementation:
  https://github.com/Comfy-Org/ComfyUI/blob/master/comfy/ldm/wan/model_animate2.py

Wan-Animate-2 was released on 2026-08-07. It is a 14B end-to-end animation
model that consumes a reference image and the raw driving video directly. It
does not require the intermediate pose/keypoint extraction used by the earlier
Wan Animate pipeline. Text conditioning describes character/background and can
control viewpoint independently of the driving video.

## Official Comfy package receipt

The official workflow uses the base INT8 ConvRot model plus the LightX2V
distillation LoRA. This is distinct from the upstream repository's standalone
distilled checkpoint.

| Role | File | Bytes | SHA-256 |
|---|---|---:|---|
| DiT | `diffusion_models/wan_animate_2_int8_convrot.safetensors` | 16,653,175,528 | `0580ecdd65e47e97c30df9670d13a6c4a131d26de5a1faf2ccc78392d5167584` |
| Distillation LoRA | `loras/lightx2v_I2V_14B_480p_cfg_step_distill_rank64_bf16.safetensors` | 738,005,744 | `85c4a61c30e0497aa44b91d93a893b624708461a56fe5485183b28fa07e2dfb3` |
| Text encoder | `text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors` | 6,735,906,897 | `c3355d30191f1f066b26d93fba017ae9809dce6c627dda5f6a66eaa651204f68` |
| Vision encoder | `clip_vision/clip_vision_h.safetensors` | 1,264,219,396 | `64a7ef761bfccbadbaa3da77366aac4185a6c58fa5de5f589b42a65bcc21f161` |
| VAE | `vae/Wan2_1_VAE_bf16.safetensors` | 253,806,278 | `1ab9a32cc2c740f6e39d80d367ce5dcc28db8c71b79b28670546b8973e9d75f9` |

Total package size: **25,645,113,843 bytes (23.88 GiB)**.

The three pre-existing supporting files matched the official hashes. The
missing INT8 ConvRot model and VAE were downloaded from the official Comfy-Org
repository to `.part` files, size/hash checked, and then renamed atomically.
No old weights were removed.

The VAE names are distinct and both files are preserved:

- Existing Wan Animate v1 VAE:
  `vae/wan_2.1_vae.safetensors`, 253,815,318 bytes,
  SHA-256 `2fc39d31359a4b0a64f55876d8ff7fa8d780956ae2cb13463b0223e15148976b`.
- Wan-Animate-2 candidate VAE:
  `vae/Wan2_1_VAE_bf16.safetensors`, 253,806,278 bytes,
  SHA-256 `1ab9a32cc2c740f6e39d80d367ce5dcc28db8c71b79b28670546b8973e9d75f9`.

The candidate DiT also uses its own filename
`diffusion_models/wan_animate_2_int8_convrot.safetensors`; it did not replace
the existing Wan Animate model.

Post-download free space on `R:` was independently observed as
**119,071,592,448 bytes** (about 110.9 GiB), sufficient for an isolated runtime
and test artifacts without deleting the working Wan Animate v1 package.

## Sampling and workflow contract

The official Comfy workflow uses:

- `wan_animate_2_int8_convrot.safetensors` plus the LightX2V distillation LoRA;
- LCM sampler, simple scheduler, six steps;
- 81-frame windows, roughly 3.4 seconds at 24 fps;
- `WanAnimate2ToVideo` for direct reference-image/driving-video conditioning;
- `WanAnimate2Cache` on CPU to reduce VRAM spikes;
- one-frame overlap for manually chained continuation windows.

The upstream standalone distilled model documents a different contract:
10 steps, Euler, guidance 1/no CFG. These settings must not be mixed with the
official Comfy base-plus-LoRA package.

## Hardware risk

Upstream defaults are tuned for 720p on 8× A800 GPUs; 480p was tested on 2×
A800 GPUs. The official Comfy INT8/offload path is intended to reduce that
requirement, but a single RTX 4090 remains unproven for this workflow.

The candidate machine has 24 GiB VRAM and about 63 GiB system RAM. The 15.5 GiB
INT8 DiT plus encoder, VAE, cache and activations may fit only with CPU
offloading. Initial acceptance should therefore use one 480p/81-frame canary
with memory telemetry. Failure must leave the current Wan Animate workflow
untouched.

## ComfyUI compatibility gap

The live portable checkout is ComfyUI **0.21.1**, commit
`26515acd23fa291a8f5ab53c5997258598de0701`, dated 2026-05-13. It is 770 commits
behind the inspected current master and does not contain
`comfy/ldm/wan/model_animate2.py`.

Native Wan-Animate-2 support entered ComfyUI in commit
`a464ac33588ae182f81a090d910cfbf21e255b73` on 2026-08-06. The isolated
candidate checkout is:

```text
R:\autorig\.codex_tmp\wan-animate2-comfy
commit e638023d54497dbe0579565e5de4bb7076899592
ComfyUI 0.37.0
```

The live embedded Python is 3.13.12 with PyTorch 2.11.0+cu130. The new checkout
cannot run against the old dependency set: old `comfy-kitchen` lacks the
ConvRot layout and old `comfy-aimdo` lacks `comfy_aimdo.storage`.

An isolated overlay at
`R:\autorig\.codex_tmp\wan-animate2-python-overlay` contains:

- `comfy-kitchen==0.2.35`
- `comfy-aimdo==0.5.5`
- `av==18.1.0`

With the candidate checkout and overlay prepended to `sys.path`, the current
embedded Python passed ComfyUI's quick CI boot and successfully imported:

```text
WanAnimate2Model
WanAnimate2ToVideo
WanAnimate2Cache
```

The complete current requirements also expect newer frontend, workflow
template and embedded documentation packages, plus `comfy-angle`. The quick
backend check warned about those gaps; it did not mutate the live runtime.

## Safe next step

1. Build a separate current portable/runtime directory under the project.
2. Install the full pinned current requirements into that isolated runtime.
3. Admit only the custom nodes required by the official workflow; do not copy
   the whole live custom-node tree blindly.
4. Launch it on a separate localhost port while the production Comfy process is
   idle, using separate user/temp/output paths.
5. Validate native `object_info` for every node in the official workflow.
6. Run one 480p, 81-frame canary with VRAM/RAM/elapsed-time telemetry and inspect
   the continuous video, not only terminal status.
7. Preserve and re-test the current Wan Animate workflow before considering a
   worker capability change.

No production replacement, capability advertisement, or live restart is
authorized by this preparation alone.

## Isolated CPU validation server

An isolated CPU-only server is running for schema validation:

```text
URL: http://127.0.0.1:8990
PID: 54268
ComfyUI: 0.37.0 / e638023d54497dbe0579565e5de4bb7076899592
custom nodes: isolated compatibility set only
```

Its input, output, temp, user database and logs are all under
`R:\autorig\.codex_tmp\wan-animate2-runtime`. It reads the existing model
directories through `wan-animate2-extra-model-paths.yaml`; it does not write to
the live ComfyUI runtime directories.

Stop only this isolated process with:

```powershell
Stop-Process -Id 54268
```

The candidate API graph is
`.codex_tmp/wan-animate2-runtime/wan_animate2_candidate_api.json`. It uses the
official Comfy package contract: INT8 ConvRot model plus distillation LoRA,
native `WanAnimate2ToVideo`, CPU/int8 `WanAnimate2Cache`, static 21-frame
context windows, LCM/simple at six steps, and one 81-frame 832×480 window. It
contains no DWPose or other intermediate pose extraction.

`validate_candidate.py` checked the graph against the live isolated
`object_info`: all 27 node classes and required inputs exist, all connection
types match, and all five exact model files are visible to ComfyUI. This is a
schema/load-path validation.

## Isolated GPU canary

After the production 4090 queue was confirmed empty, the stock worker was
parked and drained through `worker-4090.ps1 -Mode Stop`. The CPU validation
process was stopped, and the same isolated checkout was launched on GPU at
port 8990. Exactly one prompt was submitted:

```text
prompt_id: 7a69f078-98ac-4c7b-9f7e-72382ce72df7
candidate PID: 11204
reference: avatar_identity_maya_canonical.png
driving video: seated-speaking-97f-24fps.mp4
output window: 81 frames, 832x480, 24 fps
```

The prompt completed successfully. Comfy execution timestamps measured
249.43 seconds. Resource telemetry observed:

| Metric | Baseline | Peak during canary | Approximate increase |
|---|---:|---:|---:|
| GPU memory used | 3,301 MiB | 19,710 MiB | 16,409 MiB |
| System RAM used | 15,325 MiB | 50,514 MiB | 35,189 MiB |

Minimum observed headroom was approximately 4.4 GiB VRAM and 12.5 GiB RAM.
The job did not OOM.

Artifact receipt:

```text
file: wan2_maya_qwerty_seated_81f_832x480_00001_.mp4
frames: 81
dimensions: 832x480
fps: 24
duration: 3.373 seconds
bytes: 354,955
sha256: 1ba1556e2cebaf2a3287d7d03927be0c7e1b9bf0a551accb8a48292af35f1368
```

Visual review of frames 0/20/40/60/80 showed strong Maya identity retention,
the requested seated speaking and hand motion, and no obvious extra limbs.
Adjacent-frame mean absolute difference had minimum 0.61, median 5.79 and
maximum 15.14, with zero near-duplicate pairs below 0.1; the clip contains
continuous motion rather than a frozen frame sequence.

The mandatory restoration path completed after the artifact was durable:
candidate PID 11204 and port 8990 were stopped, the stock worker was restored
with `worker-4090.ps1 -Mode Start`, port 8988 and reverse tunnel 19409 answered,
the Renderfin registry showed `worker-4090` online with no current task, and all
previous workflow tokens were restored. No production Wan-Animate-2 capability
was registered.

## 97-frame preflight

The native node does not impose an 81-frame hard limit. Its `length` input
accepts a four-frame cadence and computes:

```text
latent_length = ((length - 1) // 4) + 1
```

The reference image is one additional latent frame at the front of the
generation branch. Therefore 81 requested frames become 21 motion latent
frames plus one reference slot (22 total), while 97 requested frames become 25
motion latent frames plus one reference slot (26 total). `model_animate2.py`
explicitly requires pose latent frames to equal generation frames minus that
reference slot.

With static context windows of length 21 and overlap 8, 26 latent frames still
produce exactly two windows: indices 0–20 and 5–25. The static-window source
moves the final window backward to retain full context length. A direct
97-frame request is therefore shape-compatible and does not require an
81-frame continuation seam.

The prepared graph
`.codex_tmp/wan-animate2-runtime/wan_animate2_candidate_97f_960x544_api.json`
uses 97 frames at internal 960×544. All 27 nodes, connections, required inputs
and five model filenames pass the CPU `object_info` validator. Exact delivery
at 960×540 should be a post-decode resize, matching the product rule.

The 81-frame canary peaked at 19,710 MiB GPU and 50,514 MiB RAM. Both graphs
use two 21-frame context windows, so the cache window count does not increase.
Pixel area rises by about 30.8%; total latent frames rise by 18.2%. A
conservative preflight estimate is 19.7–21.5 GiB GPU used and 54–58 GiB system
RAM used. This estimate is not acceptance evidence; a real 97-frame canary
still requires telemetry.

## ComfyUI 0.37 compatibility preflight

The isolated runtime contains copies of the custom-node packages needed by the
currently advertised 4090 workflows:

- ComfyUI-LTXVideo
- comfyui_controlnet_aux
- ComfyUI-Video-Depth-Anything
- comfyui-kjnodes
- comfyui-videohelpersuite
- srl-nodes
- ComfyUI-Helper-Nodes

The live packages were not modified. The original LTXVideo copy at commit
`229437c6` was incompatible with current ComfyUI because it imported the removed
`interleaved_freqs_cis` API. The isolated copy was replaced with upstream
commit `dfb2786749af36f200ea023388dc729a3e106b42`. Its `colour-science` and
`openimageio` dependencies, plus Helper Nodes' `whratio` and `piexif`, were
installed only into the isolated overlay.

The overlay also pins the current core support packages:

- `comfy-kitchen==0.2.35`
- `comfy-aimdo==0.5.5`
- `av==18.1.0`
- `comfyui-frontend-package==1.53.6`
- `comfyui-workflow-templates==0.11.66`
- `comfyui-embedded-docs==0.5.12`
- `comfy-angle==0.1.1`

The CPU-only compatibility server was validated at
`http://127.0.0.1:8990`, PID 54268. It was stopped before the isolated GPU
window and is not currently running.

All selected custom-node packages import without an `IMPORT FAILED` error. The
preflight expanded every currently advertised workflow to its runtime file,
including the `gen_image.json` override, then checked the isolated server's
`object_info`: 14 workflows, 52 unique node classes and 16 literal model files
were checked, with zero missing classes and zero missing filenames. Key nodes
confirmed include `LTXAddVideoICLoRAGuide`, `LTXICLoRALoaderModelOnly`,
`LTXVTiledVAEDecode`, `DWPreprocessor`, `VideoDepthAnythingProcess`,
`ReferenceLatent`, `WanAnimate2ToVideo` and `WanAnimate2Cache`.

`comfyui_controlnet_aux` warns that ONNX Runtime acceleration is unavailable
inside the isolated overlay and falls back to OpenCV/CPU for DWPose. The node
imports and its existing TorchScript weights resolve, but performance must be
measured before production replacement. This warning does not affect native
Wan-Animate-2, which consumes the raw driving video and uses no DWPose node.

## 97-frame isolated GPU acceptance

After CPU schema validation and confirmation that the production queue was
empty, the stock worker was parked and drained. Exactly one isolated GPU
prompt was submitted:

```text
prompt_id: 765af54f-ea9a-4509-9dc0-85b36e3badbc
candidate PID: 37148
reference: avatar_identity_maya_canonical.png
driving video: seated-speaking-97f-24fps.mp4
internal sampling: 97 frames at 960x544
delivery: native ImageScale to 960x540 before CreateVideo
```

The job completed successfully. Comfy execution timestamps measured 310.12
seconds; the telemetry poller measured 275.8 seconds after submission setup.

| Metric | Baseline | Peak during canary | Approximate increase |
|---|---:|---:|---:|
| GPU memory used | 3,380 MiB | 19,619 MiB | 16,239 MiB |
| System RAM used | 15,649 MiB | 56,897 MiB | 41,248 MiB |

The graph used the expected 26-frame latent shape and logged exactly two static
windows: 0–20 and 5–25. It did not OOM.

Artifact receipt:

```text
file: wan2_maya_qwerty_seated_97f_exact_960x540_00001_.mp4
frames: 97
dimensions: 960x540
fps: 24
duration: 4.040 seconds
bytes: 502,493
sha256: bce5aaae9f4602f82f93deb3c72928bda202cbdbcfa5b363a9ecf000103e8138
```

All 97 frames were rendered into an 11×9 contact sheet and reviewed. Maya's
identity, clothing, background, face and two hands remain coherent through the
full motion. Adjacent-frame mean absolute difference had minimum 0.57, median
5.53 and maximum 15.61, with zero near-duplicate pairs below 0.1; there is no
frozen segment or continuation seam.

The candidate GPU process was stopped after the terminal artifact. The stock
worker was restored with `worker-4090.ps1 -Mode Start`; port 8988 and reverse
tunnel 19409 answered, the queue was empty, the Renderfin registry showed the
worker online with no current task, and all 15 prior workflow tokens were
restored. Port 8990 is closed. No Wan-Animate-2 production capability was
registered.
