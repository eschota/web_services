# Hair Lab / Real Hair — public development handoff

Updated: **2026-10-12 12:18 Asia/Novosibirsk**

Hair Lab is an optional AutoRig V3 tool for analyzing existing hair and creating game-ready low-poly hairstyles after the base model analysis and rig are available. Real Hair is the reusable Unity/URP technology developed from the same guide, card and dynamics work. Neither is a completed customer feature or released product yet.

This page is the public-safe feature checkpoint. It contains measured progress, current limits and TODOs. Internal operations, private assets and customer source identities are intentionally excluded.

## Current stage

| Area | State | Evidence and limit |
|---|---|---|
| Research and design | **Complete for the first implementation plan** | Five candidate methods, source review, graph/tool contract, quality gates and dataset boundaries are documented in [research](research.md) and [integration](integration.md). |
| Real-model admission | **Measured, not accepted for automatic replacement** | Five recent V3 models were inspected. Four have fused or unresolved hair; one has a separate hair partition suitable for diagnostics. A fused-bob packet now has native head views and a bounded SAM2 probe, but automatic Apply remains **0/5**. See [real cases](real-cases.md). |
| GPU projection | **Diagnostic kernel verified** | Six-view CUDA raster on the heaviest tested Head-weight ROI measured p50 0.880 ms. Cold source extraction measured 14.618 s. These are separate stages, not end-to-end hairstyle generation latency. See [real benchmark](real-benchmark.md). |
| Hair geometry | **WIP** | Card hybrids remain rejected. Source-normal transfer onto a 2,500-triangle source shell uses 7,500 render-corner normals mapped to 1,252 physics vertices and looks less faceted. Art, cards and atlas remain below acceptance. |
| Dynamics | **r7 verified as a bounded shell test** | Full-shell native CPU Burst deformation, copies and mesh upload measured p50/p90 **1.9611/2.0725 ms** over 180 frames. Guide simulation is a separate **3.1664 ms** stage. Core/Burst parity reached 2.18e-7 m position and 0.00399° normal direction. Current source has 33 core tests. See [real motion](real-motion.md). |
| Head collision | **Open** | No accepted true-head collider exists. The tested closed proxy overfills concavities and is not accepted as production collision geometry. |
| Materials | **Technical intake checked; art not accepted** | Original atlas and Full/Light pair passed 56 importer checks and actual Unity usage/stand captures. Sparse test ribbons are not a hairstyle. The material remains unadmitted and unexportable pending art and real-head validation. |
| Task integration | **Source-only** | The restricted MT operation and an owner-only inspector implementation exist in source. The inspector has 23 passing focused tests, but both remain under review, unmounted and unable to generate or Apply hair in customer tasks. |
| Real Hair product | **Pre-release development** | Portable guide/card and dynamics technology is being validated for Unity/URP. No finished package, store submission or release is claimed. |

## Latest evidence

The first real dynamics run exposed 178.77% segment stretching that smaller numerical cases missed. The corrected run measured 0.004725% maximum segment stretch with zero root drift over 180 frames. This validates the corrected mechanics for that frozen input; it does not validate finished hair art, collision or five hairstyle presets.

The r6 full-shell comparison confirms that the native Burst path matches the reference implementation over all 180 frames. The shell timing excludes guide simulation, capture, rendering, atlas work and network/task overhead.

Normal validation reports eight disagreements in the source shell and nine after deformation, leaving one new dynamic release blocker. Follow-up split-normal and shape-fitting paths remain experimental: fewer mismatches cannot justify visible shading pops. The r7 default rerun produced exactly the same180 frames as r6. Neither experiment replaces this baseline.

A second registered packet for a fused bob contains source-bound geometry and aligned ID, depth, normal and color crops. One free-farm Vision request completed in19.54s, but its472-triangle diagnostic missed bangs and lower locks. Root inspected and rejected the registered overlay; original hair stays and Apply remains disabled. DEV6771 contains this failure evidence, not accepted segmentation.

The fused-bob continuation renders four native 512 px close-ups in **3.60 s**. A local SAM2-tiny CPU probe processed all four views in **2.73 s**. Its independent sparse reviewer set contains 30 hair and 16 non-hair anchors; the conservative fused result recalls **22/30 hair** while preserving **16/16 non-hair** anchors. This is a small review sample, not dense ground truth. Fringe, lower locks and uncertain face boundaries remain incomplete, so Apply is still false and overall admission remains **0/5**.

For the separate-hair case, source-normal transfer onto QEM2500 produces a visibly less faceted shell. The experimental Unity r3 frame fit lowers edge strain from **12.34% to 6.90%**, but managed runtime rises from **45.54 ms to 75.87 ms** and maximum temporal normal step grows from **26.66° to 41.09°**. A visible face rotates about42° between the first two simulated frames; it is not a tiny-triangle artifact. This is **not promoted**. All180 actual input/output frames are cached for causal replay. DEV6778 shows chronological motion with a fixed camera, not a turntable or real-time FPS claim.

Two 2D hairstyle concepts are owner-approved as visual references only. Their approval does not extend to geometry, segmentation, dynamics or materials. The original owned 3D character remains queued; no completed 3D character is claimed.

DEV6767 contains the r6 motion evidence. It was delivered for review; delivery is not approval.

## Quality boundary

Hair Lab is not ready to advertise automatic hairstyle replacement. Current evidence proves individual geometry, projection and dynamics stages. It does not yet prove:

- reliable hair separation on fused meshes;
- five accepted hairstyle presets on varied real heads;
- a true head, ears, face and shoulder collision solution;
- final atlas and material quality;
- stable attachment under the complete animation library;
- end-to-end task latency, cancellation, resume, Apply and undo;
- finished exports or a releasable Real Hair package.

Customer uploads are regression inputs only and do not automatically become training data or redistributable product assets.

## Prioritized TODO

1. Reproduce and fix the visible first-step geometry jump using recorded Unity inputs, then repeat180-frame validation before changing the verified r7 baseline.
2. Improve fused-hair segmentation beyond the sparse 22/30 hair anchor result while preserving non-hair exclusions; keep hierarchy/material names as evidence, not geometric truth.
3. Build and validate a true-head collider with explicit face, ears, scalp and shoulder exclusions.
4. Finish the atlas, alpha, tangent and normal pipeline; compare it in fixed cameras and motion.
5. Improve the accepted guide/card basis on real heads, then create five distinct presets plus **Keep current hair**.
6. Finish review of the source-only MT operation and owner inspector; mount the task tool only after parser, authority, provenance, progress, cancel/resume and undo checks pass.
7. Run full motion QA and publish per-stage timings: extraction, segmentation, guides, cards, atlas, bind, simulation, validation and viewer publication.
8. Complete Real Hair Unity/URP import, presets, studio, documentation, package validation and visual approval before any release claim.

## Public references

- [Research and architecture](research.md)
- [Reconstruction sources](reconstruction-sources.md)
- [Task and graph integration](integration.md)
- [Five real-model cases](real-cases.md)
- [Real-model benchmark](real-benchmark.md)
- [Dynamics and r6 shell evidence](real-motion.md)
- [Public AutoRig handoff](/handoff.md)
