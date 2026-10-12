# Hair Lab / Real Hair — public development handoff

Updated: **2026-10-12 09:55 Asia/Novosibirsk**

Hair Lab is an optional AutoRig V3 tool for analyzing existing hair and creating game-ready low-poly hairstyles after the base model analysis and rig are available. Real Hair is the reusable Unity/URP technology developed from the same guide, card and dynamics work. Neither is a completed customer feature or released product yet.

This page is the public-safe feature checkpoint. It contains measured progress, current limits and TODOs. Internal operations, private assets and customer source identities are intentionally excluded.

## Current stage

| Area | State | Evidence and limit |
|---|---|---|
| Research and design | **Complete for the first implementation plan** | Five candidate methods, source review, graph/tool contract, quality gates and dataset boundaries are documented in [research](research.md) and [integration](integration.md). |
| Real-model admission | **Measured, not accepted for automatic replacement** | Five recent V3 models were inspected. Four have fused or unresolved hair; one has a separate hair partition suitable for diagnostics. Automatic Apply remains **0/5**. See [real cases](real-cases.md). |
| GPU projection | **Diagnostic kernel verified** | Six-view CUDA raster on the heaviest tested Head-weight ROI measured p50 0.880 ms. Cold source extraction measured 14.618 s. These are separate stages, not end-to-end hairstyle generation latency. See [real benchmark](real-benchmark.md). |
| Hair geometry | **WIP** | The best current hybrid keeps a 2,500-triangle source shell and adds 160 cards, 4,420 triangles total versus 5,680 in the source hair. Fixed-camera silhouette IoU is 0.963 front, 0.955 side and 0.966 back. Visible tips, normals, atlas and art direction still need work. |
| Dynamics | **r6 verified as a bounded shell test** | Full-shell native CPU Burst deformation, copies and mesh upload measured p50/p90 **1.9485/2.0051 ms** over 180 frames. Guide simulation is a separate **3.1263 ms** stage. Core/Burst parity reached 2.18e-7 m position and 0.00399° normal direction; 28 core tests passed. See [real motion](real-motion.md). |
| Head collision | **Open** | No accepted true-head collider exists. The tested closed proxy overfills concavities and is not accepted as production collision geometry. |
| Materials | **Technical intake checked; art not accepted** | Original atlas and Full/Light pair passed 56 importer checks and actual Unity usage/stand captures. Sparse test ribbons are not a hairstyle. The material remains unadmitted and unexportable pending art and real-head validation. |
| Task integration | **Source-only** | MT operation source commit `71fac68` exists, but it is not mounted in the task/API and has not changed customer tasks. Parser hardening and Vision scheduling remain in progress. |
| Real Hair product | **Pre-release development** | Portable guide/card and dynamics technology is being validated for Unity/URP. No finished package, store submission or release is claimed. |

## Latest evidence

The first real dynamics run exposed 178.77% segment stretching that smaller numerical cases missed. The corrected run measured 0.004725% maximum segment stretch with zero root drift over 180 frames. This validates the corrected mechanics for that frozen input; it does not validate finished hair art, collision or five hairstyle presets.

The r6 full-shell comparison confirms that the native Burst path matches the reference implementation over all 180 frames. The shell timing excludes guide simulation, capture, rendering, atlas work and network/task overhead.

Normal validation reports eight disagreements in the source shell and nine after deformation, leaving one new dynamic release blocker. A follow-up normal fix now exists in source only. It is **pending temporal-continuity review and a Unity rerun**: a lower mismatch count cannot justify visible shading pops. R6 remains the latest verified result and the new fix is not promoted to verified status here.

A second registered packet for a fused bob contains source-bound geometry and aligned ID, depth, normal and color crops. It is an input packet for a future segmentation pass, **not a hair mask or completed segmentation**.

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

1. Independently review the source-only normal fix and repeat the same 180-frame Unity validation before changing the verified baseline.
2. Produce source-bound hair segmentation for the four fused/unresolved real cases; keep hierarchy/material names as evidence, not geometric truth.
3. Build and validate a true-head collider with explicit face, ears, scalp and shoulder exclusions.
4. Finish the atlas, alpha, tangent and normal pipeline; compare it in fixed cameras and motion.
5. Improve the accepted guide/card basis on real heads, then create five distinct presets plus **Keep current hair**.
6. Mount the restricted Hair Lab MT operation behind the task tool only after parser, authority, provenance, progress, cancel/resume and undo checks pass.
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
