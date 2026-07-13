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

Every output directory is immutable-by-convention and contains canonical JSON,
compressed arrays, one mask per frame, a contact sheet, ffprobe evidence,
diagnostics, and a SHA-256 manifest. Use a fresh output directory for every
candidate; there is deliberately no overwrite switch.

## Current exact boundary

- The current external eight-frame horse clip is a static runtime smoke fixture. It proves deterministic TAPNext++ -> SAM 2.1 -> VDA execution, not acceptable walk motion or fitting quality.
- VDA produces relative, unscaled diagnostics. The runtime therefore leaves canonical metric `depth` observations empty; it does not pretend those arrays are camera-Z.
- The runtime does not infer hoof contacts. Canonical `contacts` remain empty until a separate measured contact detector/reviewer supplies them.
- SAM provides one foreground animal mask. This is not an articulated differentiable silhouette or per-limb segmentation model.
- Tracking/masking/depth observations alone do not create a production clip. A real LTX horse-motion candidate, temporal fitting, motion-authoring export and reviewed acceptance gates are still required.
