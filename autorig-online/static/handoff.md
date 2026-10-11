# AutoRig V3 — live development handoff

Updated: **2026-10-12 05:40 Asia/Novosibirsk**<br>
Production snapshot: **2026-10-11 22:40 UTC**

This is the public coordination checkpoint for AutoRig development. It contains current production facts, active work and the next tasks. It deliberately contains no credentials, private customer data, internal network addresses or licensed assets.

## Product direction

AutoRig is moving to one V3 task flow. One task owns one viewer and one session agent. The input can be a 3D mesh, image, video or text. The session agent selects tools for analysis, generation, rigging, skinning, retargeting, QA, scene presentation and export. Astra coordinates service-wide work; the session agent stays responsible for one task.

The task must become interactive quickly. The target from worker pickup to an animated rig in the viewer is **60 seconds**. Heavy QA, exports, preview videos and other optional results continue in the background and report progress to the same task.

## Live production state

| Area | State | Evidence / limitation |
|---|---|---|
| Task page | **Live on V3 for all web task opens** | `/api/task-page/live` reports `mode=all`. Telegram WebApp routing is still disabled in that live switch. |
| Intake | **V3-only direction is active** | New rig work is routed to V3; old completed tasks remain accessible. Format coverage and retry paths still require regression checks. |
| Viewer | **Live, actively developed** | Current viewer shows the mesh, rig, clips, processing stages, scenes and the session-agent UI. Blueprint bone editing and clean capture mode remain in progress. |
| Rig and skin | **Working, quality work continues** | Humanoid, multi-limb and serpent paths exist. Geodesic skinning, leg fitting, joint judging and numeric QA are installed, but clothing, hair, posed inputs and unusual proportions still produce open regressions. |
| Live progress | **Working** | The viewer receives source, analysis, voxel, skeleton, weight, retarget, QA and publication events when a stage produces them. |
| Fleet | **Degraded at this snapshot** | Public fleet status reported new tasks paused, no converter immediately able to accept work, 1 Renderfin job pending, 3 rendering and 5 converter jobs queued. Several V3-capable nodes were online, but converter admission was blocked. Recheck the live API before acting. |
| Tool catalogue | **Public contract** | Session tools belong in [the tool catalogue](/dev/tools). A tool is not complete until its options, authority, outputs and undo/validation behavior are listed there. |

Live machine-readable status:

- [Fleet status](/api/fleet?format=text)
- [Task-page rollout](/api/task-page/live)
- [Session tool catalogue](/dev/tools)

## Work in progress

- **Blueprint viewer:** manual joint editing, saved rig versions, retarget offsets, compact UI and clean preview capture from the real V3 viewer.
- **Rig quality:** fix open regression classes for shoulders, elbows, feet, short/stubby legs, posed characters, props, clothing layers and asymmetric meshes. Every bad model becomes a repeatable corpus case.
- **Skinning:** keep weights constrained by mesh topology, improve contact boundaries, and give clothing and hair explicit layers, bones and QA instead of treating them as body vertices.
- **Vision QA:** validate body class, limb count, feet/heels, hands/fingers, clothing/submeshes and joint placement from source-bound projections. Image suggestions do not become authoritative geometry until tied to the exact source and verified numerically.
- **Scenes and media:** lazy-load environments after the fast black-stage presentation, produce clean social previews from the actual animated viewer, and preserve per-user scene and post-processing settings.
- **Fleet:** restore healthy converter admission node by node, keeping customer tasks first and proving every release with the fast regression gate.

## Hair Lab — new V3 tool

Hair Lab is an optional background tool available after the base analysis and rig are ready. A head icon in the 3D viewer will open five choices plus **Keep current hair**. The user or session agent can start it.

Planned graph inputs are source-bound head projections and analyzer passes: color/albedo, depth, normals, mask, material ID, object ID, triangle ID, camera data, hierarchy/submesh names and the head/scalp geometry. The graph must first decide whether the input already contains hair, identify the hairstyle and separate hair from head, ears, clothing and accessories.

The research target is game-ready early-2000s style hair with a small polygon budget, textured strands/cards, a clean scalp boundary and lightweight secondary motion. Candidate implementations will compare:

1. fitted low-poly cap with layered texture cards;
2. GPU guide curves converted to clustered cards;
3. voxel or signed-distance hair volume reduced to shells/cards;
4. point-cloud or splat guides reduced to polygon strips;
5. farm-generated hairstyle mesh followed by fast retopology and fitting.

Quality comes before client-side execution. The fastest method that preserves silhouette, parting, bangs, ponytails and texture direction wins per hairstyle class. Full 3D generation remains a fallback for complex styles and dataset production, not the default for every task.

The detailed technology review, benchmark protocol and dataset plan will be published at [Hair Lab research](/static/docs/hair-lab/research.md). **The linked research document is still a TODO as of this checkpoint.**

Hair Lab acceptance criteria:

- five visibly different presets derived from the analyzed hairstyle;
- stable front, side, back and top silhouette without scalp leaks;
- exact attachment to the current head and ears with preserved accessories;
- texture atlas and alpha handling suitable for the V3 viewer and exports;
- optional bones or guide dynamics with bounded motion and no head penetration;
- deterministic receipt binding source, projections, parameters and output hashes;
- benchmark on varied short, long, tied, layered and textured-card hairstyles;
- background execution with progress, cancellation, undo and comparison in the task.

## Prioritized TODO

### P0 — service and correctness

- Recheck the live fleet and remove the current admission blockers without losing queued work.
- Keep the animated-rig milestone within 60 seconds; move optional analysis and deliverables behind it.
- Close the highest-frequency rig regressions with production corpus cases and before/after metrics.
- Finish Blueprint joint editing and save a new, reversible rig version with retargeted clips.
- Publish the Hair Lab research, choose a measurable baseline, then implement one fast end-to-end preset before expanding to five.

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
