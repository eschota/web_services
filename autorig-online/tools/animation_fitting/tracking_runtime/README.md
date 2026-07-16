# Animal tracking runtime

This package turns an LTX horse/animal MP4 plus an immutable actionless fitting
bundle into optimizer-compatible observations. Production inference uses
commit-pinned official checkouts of Google DeepMind TAPNext++, Meta SAM 2, and
optional Video Depth Anything Small. Model weights and the isolated Python venv
live outside this repository under `R:\ComfyUI-data\autorig-fitting\runtimes`.
Each checkpoint is hash/size pinned and linked to the pinned source repository
and its license file for provenance. That linkage is deliberately recorded as
**not** an independent assertion about the checkpoint weights' license.

The runtime fails before publishing `observations.json` when the first frame no
longer matches the canonical render, a commit/license/checkpoint hash changes,
tracks or masks are incomplete, visibility or visible-track confidence
collapses, points leave the frame, mask area jumps, or point/mask consistency
is too low. Both a minimum visible confidence and a median visible-confidence
threshold are configurable; zero-confidence visible tracks fail by default.
Relative VDA depth is saved only in `observations.npz`; it is never mislabeled
as metric camera depth.

The canonical bundle RGB remains the default first-frame alignment reference.
Loop candidates generated from the v11 unified browser static scene, the v12
unified browser recovery-guide scene, or the v14 lossless browser interval
guide may opt in to that bundle's browser-rendered endpoint instead. The
runtime accepts only a guide directory plus the exact lowercase SHA-256 of its
`immutable_manifest.json`; that SHA must also be present in the checked-in
authoritative allowlist. The runtime derives frame 0 and frame N-1 from the
authorized manifest.
Both endpoint PNGs must match their manifest SHA-256 and byte pins and be
byte-identical. The pinned manifest must be `PASS`, browser-only, Blender-free,
use its schema-bound `v11_unified_browser_static_scene_v1`,
`v12_unified_browser_recovery_guides_v1`, or
`v14_unified_browser_interval_guide_v1` scene contract, and link its canonical
RGB, fitting bundle, immutable manifest, source-model SHA and rig type back to
the actual actionless bundle. The v12 profile additionally requires exactly
the nine guide frames `0/6/12/18/24/30/36/42/48`, byte-identical four-hoof
recovery frames `12/24/36`, and PASS contact-cue QA showing three stance cues
at each swing apex and four at every recovery/endpoint. This override is
loop-only and never permits an arbitrary pinned image.
The exact v14 profile is separately allowlisted at
`a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb`.
It verifies all 49 browser PNGs, the PNG-in-Matroska interval video, the pose
contract, exact v12-f2 source-anchor provenance, per-frame contact-cue
visibility, deterministic rerender QA, and byte-identical endpoint/recovery
barriers before selecting frame 0.

Create the venv with system Python 3.10, install `torch==2.7.1` and
`torchvision==0.22.1` from the official `cu128` PyTorch wheel index, and then
install the non-Torch versions in `environment-lock.txt`. The three model
checkouts are imported from their commit-guarded paths rather than installed
editable, so running inference does not dirty those checkouts.
The runtime also sets and validates `CUBLAS_WORKSPACE_CONFIG=:4096:8` before
importing Torch, then enables strict deterministic CUDA algorithms.

```powershell
$python = 'R:\ComfyUI-data\autorig-fitting\runtimes\venv-py310-cu128\Scripts\python.exe'
$env:PYTHONPATH = 'R:\autorig\autorig-online\tools'
& $python -m animation_fitting.tracking_runtime doctor --with-depth
& $python -m animation_fitting.tracking_runtime observe `
  --video 'R:\ComfyUI-data\autorig-fitting\candidates\horse_walk.mp4' `
  --bundle 'R:\ComfyUI-data\autorig-fitting\horse-canonical-f1' `
  --output-dir 'R:\ComfyUI-data\autorig-fitting\observations\horse_walk_candidate_001' `
  --loop --with-depth
```

For a browser-static-scene loop, add both opt-in arguments (never just one):

```powershell
& $python -m animation_fitting.tracking_runtime observe `
  --video 'R:\ComfyUI-data\autorig-fitting\candidates\horse_walk_v11.mp4' `
  --bundle 'R:\ComfyUI-data\autorig-fitting\horse-canonical-f1' `
  --output-dir 'R:\ComfyUI-data\autorig-fitting\observations\horse_walk_v11_001' `
  --loop `
  --browser-endpoint-guide-bundle 'R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments\horse-walk-v11-browser-static-scene-guides-f2' `
  --browser-endpoint-guide-manifest-sha256 '9290e2c5c95ab0a24175f1ba873f4af6f221ce963a315e933bcc97aa540ec173'
```

For the exact v12 recovery-guide f2 bundle, use the same two arguments with:

```powershell
  --browser-endpoint-guide-bundle 'R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments\horse-walk-v12-browser-recovery-guides-f2' `
  --browser-endpoint-guide-manifest-sha256 '7484b6fe3d7e190c118b01d5baec22e4a1021647eb4145c9c74ab0daeac29451'
```

For the exact v14 lossless interval-guide bundle:

```powershell
  --browser-endpoint-guide-bundle 'R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments\horse-walk-v14-browser-interval-guide-f1' `
  --browser-endpoint-guide-manifest-sha256 'a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb'
```

Every output directory is immutable-by-convention and contains canonical JSON,
compressed arrays, one mask per frame, a contact sheet, ffprobe evidence,
diagnostics, and a SHA-256 manifest. Use a fresh output directory for every
candidate; there is deliberately no overwrite switch.

When an upstream center crop makes the full-scene scalar correlation ambiguous,
run the independent foreground gate before fitting. Its prompt box is derived
only from optical flow over decoded video pixels. Pinned SAM 2.1 selects a mask
only by its own predicted-IoU score; canonical geometry and semantic anchors are
introduced only after that mask has been frozen. The gate compares against the
high-confidence (`uint8 >= 128`) canonical silhouette and publishes immutable
mask, saliency, overlay, metrics, and hash evidence:

```powershell
& $python -m animation_fitting.tracking_runtime admit-foreground `
  --video 'R:\ComfyUI-data\autorig-fitting\candidates\horse_trot.mp4' `
  --bundle 'R:\ComfyUI-data\autorig-fitting\horse-canonical-f1' `
  --output-dir 'R:\ComfyUI-data\autorig-fitting\admission\horse_trot_001' `
  --reference-geometry-mode center_crop_cover
```

A PASS permits fitting but never approves animation quality: fixed-camera visual
review and deformation/contact QA remain mandatory.

If that independent mask proves that a default surface seed lies outside the
observed subject, `observe` may repeat `--priority-anchor-id BONE:VERTEX` to use
a pinned same-bone surface anchor. The exact IDs are recorded in first-frame
provenance; tracking thresholds remain unchanged.

## Current exact boundary

- The current external eight-frame horse clip is a static runtime smoke fixture. It proves deterministic TAPNext++ -> SAM 2.1 -> VDA execution, not acceptable walk motion or fitting quality.
- VDA produces relative, unscaled diagnostics. The runtime therefore leaves canonical metric `depth` observations empty; it does not pretend those arrays are camera-Z.
- The runtime does not infer hoof contacts. Canonical `contacts` remain empty until a separate measured contact detector/reviewer supplies them.
- SAM provides one foreground animal mask. This is not an articulated differentiable silhouette or per-limb segmentation model.
- Tracking/masking/depth observations alone do not create a production clip. A real LTX horse-motion candidate, temporal fitting, motion-authoring export and reviewed acceptance gates are still required.
