# Hair Lab — V3 project entity

Owner request: 2026-10-12. Optional low-poly hairstyle modelling after the base analysis and rig are ready; same tool for the task owner and session agent. Five user-facing style choices plus keep; up to five generator strategies. Primary target: textured polygonal locks/cards in 2000s game style, with secondary motion.

Canonical public research/design: `autorig-online/static/docs/hair-lab/research.md`, served at https://autorig.online/static/docs/hair-lab/research.md.

Public project coordination: https://www.autorig.online/handoff.md.

Research source notes: `autorig-online/docs/research/hair-reconstruction-sources-20261012.md` and `hair-integration-20261012.md`.

Runtime status: research/prototype only. There is no registered `hair_lab` task tool or executable Hair Lab `/nodes` graph yet. A standalone synthetic-head demo must never be labelled reconstruction of a customer model or a completed game-ready hair asset.

## Ownership

- Coordinator: current root AutoRig session; design, dataset specification, receipts, public research.
- Reconstruction research: `hair_research`; source evidence only.
- Integration research: `hair_integration`; source/API contracts only.
- Procedural prototype: `hair_cards_prototype`; isolated `static/hair-lab/` files.
- GPU microbenchmarks: `hair_gpu_benchmark`; `projects/hair-lab/benchmarks/`, results under project `.work/`.

No shared Unity/viewer/backend file is owned by this initial prototype. Coordinate with the existing entity owner before adding production hooks. Do not restart Astra. Do not spend on paid AI services.

## Milestones

- H0: research and design, actual state distinguished from targets.
- H1: ten synthetic variants, browser card generator/atlas and narrow measured timings.
- H2: registered real-head data and authoritative hair/scalp selection.
- H3: roots, skin, secondary dynamics, animation and export QA.
- H4: restricted `/nodes` template, task child entity, `hair_lab`, head icon, apply/undo and recovery.
- H5: volume/point/strand/Hunyuan methods evaluated on the same corpus.
- H6: original dataset and compact learned fitting, only if it improves the baseline.

Every claimed result carries exact inputs, source hashes, revisions, hardware, stage scope, measurement count and known limitations. Do not treat the historical 0/5 layer-rig checkpoint as a fresh acceptance audit.

The owner explicitly authorized this local computer for research on 2026-10-12. This permits local experiments for Hair Lab; it does not require interruption of unrelated accepted work.

## Verified checkpoint, 2026-10-12 06:39 Asia/Novosibirsk

- H0 source review and public docs complete. Public real-case results: `/static/docs/hair-lab/real-cases.md`.
- Five real public SFW V3 cases collected, exact source/rig hashes and current `needs_review` status retained in `.work/hair-lab-real-20261012/cases.json`. No model binaries in Git or training claim.
- Blender current Head-weight extraction and six-view baselines complete on all five. Corrected custom CUDA raster independently reviewed as a diagnostic benchmark; exact report/JSON public, not production hair replacement.
- Raw v1 CUDA benchmark rejected; retained v2 SHA `b7fb77ce8e4c69185f8589946fdfa59899b6bdba4eb6d68fe96987a857fe386e`. Its historical cold-named field is a component sum, not contiguous end-to-end time.
- Exact Hair primitive isolated on real case `cd073629c54afde29b03`; generated candidates below artistic acceptance. Local QA harness in `static/hair-lab/` is source-only and not deployed. Original task/source unchanged, Apply disabled. Source Hair/Face/Mask/Eyes extracted only into `.work`; do not publish its payload as a generic asset.
- Real pair/inspection media delivered to DEV, messages 6748/6749; their acceptance is not recorded. Original concepts 6741–6743 were accepted through exact DEV replies 2283–2285 (short, long, full-body T-pose r2). This approves those 2D visual targets only, not 3D/hair/skin/dynamics.
- Original full-body concept complete; ONE current free farm job `e7f282cf39704243ae7197014bd5d478` pending. Old `a923…` cancelled before dispatch; local RAM gate refused before any local submit. Follow the current job rather than creating another.
- Next: improve guides/atlas art quality on actual heads, source-bound segmentation for fused hair, eliminate cold Blender overhead with verified direct GLB extraction or a warm worker, then skin/dynamics and actual task-tool/graph registration.
