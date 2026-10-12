# Hair Lab / Real Hair — full session handoff

Updated: 2026-10-12, Asia/Novosibirsk.

## Goal and completion boundary

The active user goal includes BOTH the working AutoRig hairstyle/hair generator AND the standalone Real Hair Asset Store product with useful reusable technology, validation through DEV, maintained handoffs and an actual verified submission. Do not mark the goal complete after research, a folder, a prototype, a benchmark or a source commit.

Previous goal turn: **progress** — public handoff/research deployed, five real V3 cases tested, corrected CUDA raster independently reviewed, exact real-head Hair primitive candidates rendered and explicitly rejected for art quality, accepted original concept references saved. Own-character generation remains a verified queued job, not completed output.

## Current projects and canonical ownership

- AutoRig: `R:/autorig/autorig-online/projects/hair-lab/`. Service scheduling, Vision, source-bound segmentation, task graph/API and Python/CUDA preprocessing live here or in their existing MT owners.
- Asset: `R:/AssetStore/ASStore26/RealHair/`. New canonical C#/Unity guide/card and secondary-motion implementation, offline-capable authoring, presets, studio, documentation and package. Product handoff and SKILL are there.
- Materials: `R:/AssetStore/ASStore26/RealMaterials/`. Required atlas/material intake with exact keys, maps, URP variants and evidence. No new material is admitted yet.
- Public current progress: https://www.autorig.online/handoff.md. Research and real tests: `/static/docs/hair-lab/research.md` and `/static/docs/hair-lab/real-cases.md`.

## Verified carry-forward facts

- Five real public SFW V3 sources/current rigs in `.work/hair-lab-real-20261012/cases.json`. They are regression inputs only; do not put them in the sellable asset or claim training permission.
- Current accepted CUDA diagnostic raster raw v2 hash: `b7fb77ce8e4c69185f8589946fdfa59899b6bdba4eb6d68fe96987a857fe386e`. Warm six-view core p50 .880 ms on the heavy Head ROI; cold Blender extraction14.618s. These are stage measurements, not whole hair performance.
- Exact Hair primitive4 of `cd073629c54afde29b03` isolated; first card candidates are below art acceptance. Original model unchanged and Apply disabled. Source-only harness: `autorig-online/static/hair-lab/`.
- DEV accepted short/long/full-body 2D concepts: replies2283–2285, media `9d25d89c1329`, `95b805d330ce`, `393fc73903ab`. They do not approve 3D topology/skin/dynamics. Real candidate/benchmark media6748/6749 are not accepted.
- One current free-farm 3D job: `e7f282cf39704243ae7197014bd5d478`; poll its exact status before any action. Old `a923…` was cancelled before dispatch; no local Hunyuan job was accepted. Local RAM admission must remain enforced.
- Own DEV cursor2285 and exact receipts: `.work/hair-lab-20261012/validation-state.json`. Read direct replies only; persist new receipt/cursor before ack. Historical viewer verdicts are kept there separately.

## Current bounded work

**10:00 local continuation:** verified r6 remains the last Windows shell baseline. Independent reviewer identified introduced triangle2209 normal mismatch as a release blocker. Core now has a source-only CSR incident-frame maximin alternative (numeric introduced-set0), but reviewer requires temporal-angle/candidate-switch analysis before Burst port. Do not call this fixed in the viewer or overwrite r6 evidence. Unity/Burst currently uses the older single-frame snapshot; mixed source must be rebuilt and parity-tested after any port.

RealMaterials current source validated in `usage-r8` and `studio-r4`:56import checks, Full/Light1key,19slots0broken; pixel hashes equal root-reviewed usage-r7. Material remains pending/unadmitted/unexportable. DEV6770uid`43b4e715e4f1` shows only technical ribbons, not accepted hair. Source local commit`5f75ba0b3`, replayed publication`87c781d70` (verifypush beforeclaimlive). All root-owned captures exited; no activeUnityEditor. Core source and MT integration workers continue bounded fixes; do notduplicate.

Public `/static/docs/hair-lab/handoff.md` is now a concise sanitized derivative, **not** a byte-for-byte full-session mirror. Do notcopy this privatefile over it. Only the server audit directory receives the full internal/session/producthandoffs. Ownerconcept3Djob still queuedattempts0 at02:55UTC; no duplicate. DirectDEVcursor2285 unchanged after02:51poll; receipts6767/6770 added locally.

**2026-10-12 shell/material/API continuation (in progress checkpoint):** QEM2500 shell bound/deformed on cd073; full Windows native CPU Burst path1.9485/2.0051ms p50/p90 vs Core44.52ms,180 exact-frame parity2.18e-7m /0.00399degrees, native sentineltrue. Guide simulation3.126ms is separate. Root diagnostics12.327% edge strain, rootmask drift3.41e-8m,0degens; source normal8→dynamic9 with1 new warning, not accepted full QA. Video DEV6767 uid`0b1be8594739`, private/tracked RealHair evidence `validation/shell/`. Core28tests pass. True-head collision and five styles remain open.

Actual Face collider is open/nonmanifold (perprimitive audit, not only combined collar); derived visual hull96³ closes/overfills concavities and overlaps ~9.1% rest hair, so remains unaccepted proxy. A second real fused-bob packet f141 maps1228 Head-domain triangles,1086 directly visible (88.44%); source+rig exactgeometry, four registered ID/depth/normal/color crops. It is not yet hair segmentation.

New MT operation module `mt/hair_lab.py` +tests+`docs/hair_lab.md` published commit`71fac68`,15 tests also passed on the VPS under `/srv/autorig/audits/hair-lab-20261012/operation-tests` at nice19. No MT mount/restart/customer writes. Root-requested streaming-parser hardening and packet Vision scheduling inspection are with `hair_integration`.

Material work: `RealMaterials/Content/Intake/RealHair`, original neutral atlas, stock Lit/SimpleLit pending pair. Unity56checks/import and scoped companion stand hardware run passed. Important paid-for traps: root-v0 PNG/GetPixels vertical conventions, missing tangent basis, a stripped no-normal diagnostic variant, inward ribbon normals, scene-load destroying capture bootstrap, main studio requiring irrelevant decals/heroes, non-idempotent pedestal filtering and stale exe reuse. `usage-r3` is REJECTED; v2 source+whole-build hash guard now required. Art/shading usage remains under review; no material admission. All work/tmp stays in its product.

**2026-10-12 latest completed continuation:** source-bound five-case hair admission audited; automatic apply0/5. Actual cd073 guide export r1 failed80 collapsed tips+1hairpin; frozen r2 passes. Source-guided radial art rejected; hybrid2500tri shell+160cards totals4420 vs5680, silhouette IoU .963/.955/.966, still WIP. Corrected QEM uses one source coordinate frame and3 axis-pairs, not6 independent semantic cameras.

RealHair Unity Mono diagnostic built and ran80guides/560points/180frames from frozen external actual-head JSON. It found178.77% stretch; corrected output0.004725%, rooterror0. Solver p50/p90 3.138/3.510ms, cardbuild/upload .623/1.312ms (n180). Core25 small tests supplement the real regression, not replace it. First build41.541s, warm final7.395s engine-step, player158,025,431bytes. No head collision/final shell/material or hairstyle approval. Details: `RealHair/validation/unity/README.md` and public `real-motion.md`.

DEV6759 (`b8dd27ab36f6`) hybrid comparison and6760 (`159e7ab122bb`) actual3-second/180-frame physics clip delivered. Cursor still2285, no new direct replies at00:56UTC. Stable named workers completed these bounded steps; reuse them for follow-ups. Own-character job still queued by eligible-capacity policy, no duplicate.

Publication checkpoint: AutoRig source `5818ef7f`; RealHair local `62e3cee40`, published via clean sparse worktree as `8821bd909`, then official inventory refresh `02b61a6fe`. Do not push divergent local AssetStore main. Public Markdown live readback returned200/no-cache and identical hashes; no service restart. This goal turn is **progress**, not completion: actual AutoRig tool/UI, accepted styles/materials/collisions and verified Store submission all remain open.

- `hair_gpu_benchmark`: direct GLB Head-weight extraction on the same five real current rigs, preserving source primitive/triangle identity and comparing to the Blender reference. Goal: remove cold Blender overhead; do not substitute new synthetic inputs or fabricate parity.
- `real_hair_core`: RealHair Runtime/Core and tools/core-tests only. Deterministic guide-to-card data/builder with finite/range/topology/UV/winding tests. No Unity launches, materials, shared manifests or Portal actions.
- Root: RealHair product setup/reuse ledger, cross-repository contracts, handoffs, integration and evidence. Reuse named agents for follow-ups rather than duplicating ownership.

### Completed in this continuation

- Created canonical `RealHair` pre-portal product, SKILL, handoff, reuse-manifest, Unity target project shell, API/agent documentation and honest listing draft. Added discovery/development ledger records without inventing a Portal ID or refreshing historical portal counts.
- RealHair portable builder:11/11 numerical tests and C#9/netstandard2.1 compilation,0 errors/warnings, independently rerun by root. Fixed tangent basis, overflow guards, collapsed-tip/hairpin handling and shared obj cache. No Unity or visual acceptance claimed.
- Direct GLB Head-weight reader and11 focused format tests pass. On five real current rigs, fresh processes took .139–1.703 s (OS disk cache uncontrolled); heavy in-process p50 .357 s versus retained cold Blender14.618s. Four geometry matches; fifth retains97 duplicate source triangles that Blender removed, so overall parity remains false. Exact report: `.work/hair-lab-real-20261012/direct-reader/REPORT.md`.
- These are bounded completed source slices, not an active daemon. The full AutoRig tool, hair art quality, dynamics, Unity runtime and Store release requirements remain active TODO.
- AssetStore Git history differs materially between the user's current local main and origin/main (observed301/347 before this publication). Do not merge/reset the broad dirty checkout. Own RealHair commits were replayed in a sparse publication worktree on current origin/main, preserving the remote canonical inventory pointer and regenerating its official inventory from all63 tracked product manifests. Published remote commit `80fef64e3` includes the product and its registration. Local original commits remain preserved; only the new product/ledger work was published.
- DEV foundation status delivered as message6753 (`0c358682e297`). It records numerical-core evidence and pending Unity/visual gates, not approval. Current implementation workers finished their bounded slices; reuse their context for the next stage. Source publication worktree is `R:/AssetStore/ASStore26/.work/real-hair-publish` and was clean at publication; it is not a second canonical Unity project or a running build.

## Complete TODO

H0 research: done. H1/H2 geometry authoring and actual-head admission: incomplete. H3 shape-preserving skin/dynamics: incomplete. H4 AutoRig `/nodes` template, canonical task tool, world head icon, five choices+keep, progress/cancel/resume/apply/undo: incomplete. H5 quality corpus and additional strategies: incomplete. H6 own licensed dataset and any learned fitting: incomplete.

Real Hair must additionally finish: portable core, Unity/URP adapter, good authored presets, RealMaterials intake, runtime/editor UI, studio and continuous-motion evidence, metadata/agent guide, clean import, exact exported-package validation, independence, truthful listing/AI disclosure, YouTube/posters/Short, web demo, launch discount, Gumroad mirror, Portal upload/submission and post-release lifecycle cleanup. All are tracked in its handoff; none is waived by a green core test.

## Resumption and verification

Read current AGENTS in both repositories and the owning product SKILL. Inspect Git and actual processes before launch: a stale lock alone does not prove a running job. Local workstation research is explicitly authorized. Keep all temporary files in the respective project. Respect Unity slot ownership and preserve WIP/customer sources. No paid API calls, no Astra restart, no service restart for documentation.

Mirror public-safe status to the website and the session checkpoint to the server audit folder after material progress. Use SHA readback, do not publish raw private checkpoint content. Keep the active goal open until the full feature AND verified asset submission requirements are proven.
