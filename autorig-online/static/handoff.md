# AutoRig V3 — live development handoff

Updated: **2026-10-12 10:00 Asia/Novosibirsk**<br>
Fleet snapshot: **2026-10-12 03:00 UTC** (point-in-time, recheck before operating)

This is the public coordination checkpoint for AutoRig development. It contains current production facts, active work and the next tasks. It deliberately contains no credentials, private customer data, internal network addresses or licensed assets.

## Product direction

AutoRig is moving to one V3 task flow. One task owns one viewer and one session agent. The input can be a 3D mesh, image, video or text. The session agent selects tools for analysis, generation, rigging, skinning, retargeting, QA, scene presentation and export. Astra is the planned service coordinator, but **its runner/gateway/admin bot are stopped by the owner's cost-control order**. Do not restart or route work to Astra until that order changes. The session agent stays responsible for one task.

The task must become interactive quickly. The target from worker pickup to an animated rig in the viewer is **60 seconds**. Heavy QA, exports, preview videos and other optional results continue in the background and report progress to the same task.

## Live production state

| Area | State | Evidence / limitation |
|---|---|---|
| Task page | **Live on V3 for all web task opens** | `/api/task-page/live` reports `mode=all`. Telegram WebApp routing is still disabled in that live switch. |
| Intake | **V3-only direction is active** | New rig work is routed to V3; old completed tasks remain accessible. Format coverage and retry paths still require regression checks. |
| Viewer | **Live, actively developed** | Current viewer shows the mesh, rig, clips, processing stages, scenes and the session-agent UI. The Blueprint owner reports bone editing/save and capture mode live in the 22:45 UTC checkpoint; their continued QA remains separate from this documentation release. |
| Rig and skin | **Working, quality work continues** | Humanoid, multi-limb and serpent paths exist. Geodesic skinning, leg fitting, joint judging and numeric QA are installed, but clothing, hair, posed inputs and unusual proportions still produce open regressions. |
| Live progress | **Working** | The viewer receives source, analysis, voxel, skeleton, weight, retarget, QA and publication events when a stage produces them. |
| Fleet | **Degraded at this snapshot** | Public fleet status reported new tasks paused; f1 listed capable, f2/f13 admission blocked; 1 Renderfin pending,2 rendering and5 converter jobs queued. Recheck the live API before acting. No fleet/service changes in this Hair Lab slice. |
| Tool catalogue | **Public contract** | Session tools belong in [the tool catalogue](/dev/tools). A tool is not complete until its options, authority, outputs and undo/validation behavior are listed there. |

Live machine-readable status:

- [Fleet status](/api/fleet?format=text)
- [Task-page rollout](/api/task-page/live)
- [Session tool catalogue](/dev/tools)

## Work in progress

- **Blueprint viewer:** manual joint editing/save and capture mode reported live; continue regression QA of retarget offsets, saved rig versions and clean preview capture.
- **Rig quality:** fix open regression classes for shoulders, elbows, feet, short/stubby legs, posed characters, props, clothing layers and asymmetric meshes. Every bad model becomes a repeatable corpus case.
- **Skinning:** keep weights constrained by mesh topology, improve contact boundaries, and give clothing and hair explicit layers, bones and QA instead of treating them as body vertices.
- **Vision QA:** validate body class, limb count, feet/heels, hands/fingers, clothing/submeshes and joint placement from source-bound projections. Image suggestions do not become authoritative geometry until tied to the exact source and verified numerically.
- **Scenes and media:** lazy-load environments after the fast black-stage presentation, produce clean social previews from the actual animated viewer, and preserve per-user scene and post-processing settings.
- **Fleet:** restore healthy converter admission node by node, keeping customer tasks first and proving every release with the fast regression gate.

## Hair Lab — new V3 tool

Hair Lab is an optional background tool available after the base analysis and rig are ready. A head icon in the 3D viewer will open five choices plus **Keep current hair**. The user or session agent can start it.

Planned graph inputs are source-bound head projections and analyzer passes: color/albedo, depth, normals, mask, material ID, object ID, triangle ID, camera data, hierarchy/submesh names and the head/scalp geometry. The graph must first decide whether the input already contains hair, identify the hairstyle and separate hair from head, ears, clothing and accessories.

The research target is game-ready early-2000s style hair with a small polygon budget, textured strands/cards, a clean scalp boundary and lightweight secondary motion. Candidate implementations will compare:

1. an owned guide library fitted to the actual scalp, then textured cards;
2. the existing hair mesh plus a flow field converted to cards;
3. registered multi-view voxel/point orientation fields converted to guides/cards;
4. an existing strand groom converted to optimized cards;
5. a farm-generated hairstyle proposal followed by retopology, guides and cards.

Quality comes before client-side execution. The fastest method that preserves silhouette, parting, bangs, ponytails and texture direction wins per hairstyle class. Full 3D generation remains a fallback for complex styles and dataset production, not the default for every task.

The [Hair Lab research](/static/docs/hair-lab/research.md) is now public, with sources, five generation strategies, graph/tool contracts, QA thresholds and a dataset plan. [Local RTX 4090 microbenchmark](/static/docs/hair-lab/benchmark.md): 25 repeats per case; synthetic 256³ hull p50 0.422 ms CUDA versus 11.789 ms CPU; 100-guide ribbon construction is faster on CPU. These are isolated kernels, not arbitrary-hair reconstruction timings.

Two original farm-rendered head concepts and the full-body T-pose concept were accepted by the owner through exact DEV replies. This approves their 2D art direction only, not 3D ground truth, topology or physics. A procedural five-style prototype exists, but full task integration and physically validated dynamics are not complete.

**Latest owner steering:** immediately test on recent real V3 models instead of abstract examples, and also generate original game characters. Local workstation research is explicitly authorized. The next acceptance evidence must use real source-bound task/head geometry; synthetic figures remain technical references only. New Hair Lab features must stay asynchronous after the base analysis and rig.

Five recent real public SFW V3 cases are now loaded with exact hashes: spikes, buns, bob and dreadlocks, including a 2.01-million-triangle rig. Their current conveyor outcomes are `needs_review`. Head-weight extraction and six-view geometry benchmarks ran on all five; this is not semantic hair segmentation. One source has an exact `Hair` material partition and experimental card candidates; their visual quality is still below acceptance. A stored hair-colour/front-back discrepancy was independently confirmed by Vision. [Real-case report](/static/docs/hair-lab/real-cases.md).

An independently reviewed CUDA raster benchmark now covers all five actual Head-weight ROIs. On the heaviest ROI, six-view CUDA p50 is 0.880 ms; cold Blender extraction still takes 14.618 s. This does not establish end-to-end hair speed or quality. The first real-head hairstyle candidate was visually rejected as below the desired strand-design quality; Apply remains disabled. DEV evidence was delivered (6748/6749), not accepted.

One original full-body T-pose concept is ready. Its single current free farm-only Hunyuan job is [`e7f282cf39704243ae7197014bd5d478`](/api/gen3d/jobs/e7f282cf39704243ae7197014bd5d478), queued awaiting eligible capacity. The earlier queue entry was cancelled before dispatch while testing a local lane; local Hunyuan refused its RAM admission floor, so the free farm request was restored once. No 3D completion or reliable ETA is claimed. The customer base rig and original assets remain unchanged by this research.

Hair Lab acceptance criteria:

**Actual-model continuation:** a Unity/URP Mono diagnostic on the same real head caught178.77% segment stretching missed by small tests; the corrected180-frame run measures0.004725% maximum stretch and zero root drift.80 guides: solver p50/p90 3.138/3.510ms, cardbuild/upload0.623/1.312ms. This is mechanics only, no final hair shell or real-head collision. The improved source-shell+cards hybrid is4420triangles versus5680 and still WIP. Automatic replacement remains0/5, with four fused/unknown cases. DEV6759/6760 delivered comparison and actual motion video, not approval. [Exact new real-model evidence and TODO](/static/docs/hair-lab/real-motion.md).

**Full-shell continuation r6:** the native CPU Burst shell path measures p50/p90 **1.9485/2.0051ms**, including input/output copies and Unity mesh upload, over180 reference-comparison frames. Guide simulation is a separate **3.1263ms** stage. Core/Burst parity is2.18e-7m position and0.00399° normal direction;28 core tests pass. Normal checks report8 source disagreements and9 after deformation. The new triangle2209 disagreement is a release blocker being investigated, not accepted quality. There is still no true-head collider, the final atlas is pending and automatic Apply remains0/5. A registered fused-bob packet is input evidence, not completed hair segmentation. DEV6767 (`0b1be8594739`) was delivered, not approved. MT operation source commit `71fac68` is unmounted; parser hardening and Vision scheduling are still in progress. WebGL and complete hairstyle latency remain unvalidated.

- five visibly different presets derived from the analyzed hairstyle;
- stable front, side, back and top silhouette without scalp leaks;
- exact attachment to the current head and ears with preserved accessories;
- texture atlas and alpha handling suitable for the V3 viewer and exports;
- optional bones or guide dynamics with bounded motion and no head penetration;
- deterministic receipt binding source, projections, parameters and output hashes;
- benchmark on varied short, long, tied, layered and textured-card hairstyles;
- background execution with progress, cancellation, undo and comparison in the task.

## Real Hair — separate Asset Store product

The owner also commissioned **Real Hair**, a standalone reusable Unity product derived from Hair Lab. It is registered locally as a pre-portal development product: no uploaded package, Portal record or submission is claimed. Its intended useful parts are offline guide/card authoring, scalp/root attachments, texture controls, lightweight dynamics, presets and a studio. Target: Unity6000.3.21f1 / URP. Customer AutoRig models and private service infrastructure do not ship.

Implemented first slice: a portable C# guide-to-card builder with explicit input/options, deterministic mesh output, UV/tangent/topology checks and atomic rejection. Numerical checks11/11 PASS; C#9/netstandard2.1 build0 errors/warnings. Unity import, URP visuals, material intake, authored style quality and physics are still pending. RealMaterials is the canonical material owner.

Also measured a direct GLB Head-weight reader on the five real V3 rigs. Heavy-file extraction fell from the retained Blender14.618s to a fresh Python process1.703s (OS cache uncontrolled), or .357s repeated in-process. One case intentionally retains97 duplicate source faces that Blender discarded; overall structural parity is not declared. This is not yet the live task implementation.

The [full session handoff](/static/docs/hair-lab/handoff.md) now tracks both products through feature completion and actual verified Store submission. Remaining release work includes Unity/player QA, original styles, canonical material intake, exact-package gates, AI Ready documentation/listing, release videos/posters/Short, web demo, eligible launch discount, Gumroad mirror and submission/lifecycle evidence. The active goal is not complete.

## Prioritized TODO

### P0 — service and correctness

- Recheck the live fleet and remove the current admission blockers without losing queued work.
- Keep the animated-rig milestone within 60 seconds; move optional analysis and deliverables behind it.
- Close the highest-frequency rig regressions with production corpus cases and before/after metrics.
- Validate the deployed Blueprint joint editing and reversible save with retargeted clips.
- Hair Lab: collect recent real V3 head/hair cases, measure the actual extraction and generation path, and close visible geometry defects before advertising quality or latency. Connect the accepted baseline to the task graph and head tool.

### P1 — complete V3 behavior

- Validate five varied heavy humanoids with clothes, long hair and accessories end to end.
- Make clothing and hair explicit analyzer entities and preserve submesh/hierarchy evidence.
- Finish Telegram WebApp V3 routing and verify every creation/retry entry point uses the same task contract.
- Finish clean viewer capture for gallery, task completion and social-media preview video.
- Make session-agent chat, media, suggested actions and viewer tools observable and reliable.

### P2 — product completion

- Finish GLB, FBX, Unreal-compatible FBX and Unity character-package exports from V3 results.
- Enforce the current subscription/export policy in one server-side entitlement check.
- Expand lazy-loaded scenes, lighting and post-processing tools while keeping initial viewer load small.
- Add Hair Lab dataset generation, scoring and training only after the procedural baseline is measured.

## Collaboration protocol

1. Read this file, the repository `AGENTS.md`, the current root `handoff.md`, and the live fleet API before changing a shared component.
2. Claim one project entity in the root handoff. Do not run two agents on the same file or problem.
3. Record facts with an observed time and evidence. Mark source-only, staged, live, accepted and blocked states explicitly.
4. Add a regression case for every newly confirmed defect. A fix needs before/after metrics on the same model, pose and cameras.
5. Keep credentials, customer identity, private source files, signed URLs and internal-only asset locations out of this public document.
6. Update this public checkpoint after a material production change. Publishing this document does not itself authorize a deployment, restart, data change or customer action.

The editable source and publication procedure live in `autorig-online/docs/public-handoff/README.md` in the `web_services` repository. The public payload is `autorig-online/static/handoff.md`.
