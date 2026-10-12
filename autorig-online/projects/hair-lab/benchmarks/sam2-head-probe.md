# Real head segmentation probe — SAM 2.1 tiny

This is an AutoRig research dependency, **not** included in Real Hair's Unity package.
No paid API, training, customer source modification or automatic Apply is performed.

## Provenance

- Upstream: https://github.com/facebookresearch/sam2
- Code commit: `2b90b9f5ceec907a1c18123530e92e794ad901a4`
- Upstream LICENSE: Apache-2.0. The optional connected-component extension has its own BSD notice; it is not built here.
- Official checkpoint: https://dl.fbaipublicfiles.com/segment_anything_2/092824/sam2.1_hiera_tiny.pt
- Downloaded checkpoint SHA-256: `7402e0d864fa82708a20fbd15bc84245c2f26dff0eb43a4b5b93452deb34be69`
- Python3.10.11, CPU Torch2.5.1+cpu / torchvision0.20.1+cpu, Hydra1.3.2. All installs/cache/checkpoint/source live under project `.work/hair-sam2-20261012`; system packages were not replaced.
- `SAM2_BUILD_CUDA=0`, no CUDA extension, `device=cpu`, postprocessing disabled. Upstream loads the checkpoint with `weights_only=True`.

## Real-input contract

`sam2_head_probe.py` accepts the source GLB, a source-bound native head-closeup receipt, an earlier diagnostic seed-mask directory, and a pinned checkpoint. Every referenced closeup image must match the receipt hash. It refuses existing output revisions and nonbinary predictor masks. Output is masks, overlays, points, model scores and timings; **none is an accepted semantic receipt**.

Positive/negative points are selected from previous GrabCut observations. Face-region negatives have reserved slots: choosing only the largest negative area selected the shoulders/neck and admitted false face pixels. Reviewer annotations are never used as input points. Model scores are not measured segmentation precision.

The frozen independent `check_semantic_anchors.py` measures selected exact source triangles against sparse manually reviewed pixels from the original lit sheet. It validates source, packet, sheet, cameras, triangle-map hashes and ID bounds. Forty-six anchors on the first fused-bob case contain30 hair and16 nonhair observations. They are a limited reviewer sample, not dense ground truth or owner approval.

## Observed 2026-10-12

The earlier input was an enlarged80x78px whole-body crop. The same registered cameras now produce native512px head views without changing source triangle order. Local CPU rendering of four views took3.60s (3.98s including load/export overhead).

The SAM2 tiny CPU probe on that real bob ran at8threads. First successful pass: model init0.372s; four image encoder+decoder calls0.617/0.608/0.595/0.605s. A corrected negative-point policy measured0.811/0.669/0.603/0.648s. These are four single-view measurements, not a repeated benchmark or end-to-end service latency. GPU performance is unmeasured; the local GPU was occupied and was not interrupted.

Root inspected the native input and both overlays. The second policy preserves much more fringe and lower hair while keeping the central face clear. Eyelid/cheek boundaries still have uncertain pixels. Registered multi-view source-triangle fusion, exclusions and external anchor checks are required next; original hair remains visible. The failed initial mask-dtype run is retained separately, not counted as inference evidence.

Use the existing MT `tools/hair_lab/` projection/fusion implementation, not another independent triangle mapper. Preserve each input/output revision and hashes. Do not package model weights, private geometry, venvs or diagnostic captures into the Unity asset.
