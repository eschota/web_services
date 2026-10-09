# AutoRig development — start here after context compression

Updated: 2026-10-10, Asia/Novosibirsk. Owner-required persistent checkpoint.

Latest verified checkpoint: default Unity MT viewer now r22 with visible T/A
feet, real pointer IK move/rotate, failed-save recovery, actual private-chat foot
inspection. Targeted-foot worker is live, MT PID1232222; other production service
PIDs/Renderfin queue untouched. Canonical HANDOFF and server mirror hash match
`3f62d33dd5c86eba966d48efbe4cd7f62964442f35bc19ebf24b11f3e4b68aab`.
Default index SHA `0add54a02ba721d2fc49a9cebf1a9dbb527670e9b6f62a599a70f1f5f60eefc3`.
Bones inspection hides terrain obscuring feet and restores the environment on
exit. REST transition restores/disables FootGrounding before resetting bones,
preventing a stale animated snapshot from overwriting REST. Production browser
checks passed; these are viewer fixes, NOT producer anatomical/skin acceptance.
Current owner screenshot's static quick-foot QA fallback is under fresh backend
path investigation. Preserve existing customer artifacts and production queue.
This is NOT full producer/converter migration: body/clothing/hair acceptance0/5.
F5 streaming GPU validation is terminal2341/2341, with explicitly limited CPU
parity coverage. Real moving-bones video was delivered to /dev (uid71093d934dd1),
not approved. Verify the canonical checkpoint before another experiment. Legacykit52cc
is a proven orchestration orphan, not successful; no status/requeue mutation.

The current primary checkpoint is `R:\3d_video_motion_transfer\HANDOFF.md`
(private repo), mirrored at `/srv/autorig/audits/3d-video-motion-transfer/handoff.md`.
The owner now assigns the whole V3 rig/viewer/converter migration to this session:
live mesh-aligned bones, persistent T/A-pose IK corrections consumed by retargeting,
dedicated clothing/hair bones with Qwen attachment masks, at least five real models,
and proactive agent inspection/correction tools. Read that checkpoint first.
The converter handoff below remains the supporting GPU research checkpoint.

The primary work is GPU erosion → anatomical bones → skinning. The owner has
confirmed migrating humanoids to V3, including a non-destructive calibration
T-pose and adaptation of animation retargeting. The canonical checkpoint links
the new numbered migration plan. MT/viewer/diorama presentation is complementary,
not a substitute for a working skin. F5 stays isolated; this is not yet a
production-ready full humanoid rig or a production-default switch.

Current owner-visible priority: malformed feet and hair spikes in the actual
Unity MT viewer `/api/mt/unity/test/index.html?run=bb6d2681d1bdcf4d694f`.
Main has personally reproduced Walking and the failed bones overlay there.
Independent numeric postvalidation and parallel Vision leg QA are implemented
and tested on the real girl: all nine clips failed numerical checks; Vision's
answer was not admitted. New owner priority is clothing detection from exact
hierarchy plus isolated layer images. Source body/head, skirt and shirt are
separate; shared material does not merge the garments.42 focused tests pass.
These components are not yet mandatory production gates. Another actor edits the
private MT fast-rig; preserve its WIP. See the newest canonical section first.
Ten public sources are inventoried, but ten validated rig scenes do not yet exist.

Source e20cb33 joins existing Vision taxonomy and a fresh limb-count
observation with V3 evidence; runtime parallel orchestration is still pending.
The owner explicitly requires name/description/category/body/limb analysis
alongside geometry before scenario selection. Existing elf classification is
same-source; one completed f7-only Vision call returned2 arms/2 legs/1 head in
11.068s. Do not repeat it or mistake the observation for a validated rig.

H0 bind/H3 calibration math and real H1 tests are retained. Cardinal elf views
pass13/25 numeric joint candidates (core6/15), oblique only4/25 (core1/15).
Existing MT parts labels are now privately resealed to exact source vertex
order, but only20.62% of faces were directly observed. Next: parallel semantic
branch integration plus soft label/C2 body-proxy/correspondence, then actual
bones, GPU skin and retargeting. Full V3 humanoid success remains0/6.

Read the authoritative [converter handoff](autorig-online/backups/repair-rig-dispatch-20261006/converter-source/handoff.md)
before continuing. It contains exact commits, tested timings, private artifact
locations, blockers, next steps and restoration rules. Then read current
AGENTS.md and the renderfin-pipeline skill and verify Git / live node state.
Do not rely solely on the chat summary or assume a historical snapshot is live.

Server mirrors of that full handoff:

- VPS (`ssh autorig-vps`): `/srv/autorig/audits/animal-gpu-development/handoff.md`.
- F5 (`ssh farm-f5`): `C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\.runtime\animal_gpu_research\handoff.md`.

After each material verified milestone, blocker or next-step change, update
the canonical converter handoff and synchronize both mirrors with SHA-256
verification. Keep secrets, private model data and licensed assets out of Git.
F5 remains development-only; do not automatically re-enable production work.
Do not use the owner's worker-4090. Warm Blender is explicitly permitted.

This scoped development checkpoint does not overwrite the unrelated historical
`autorig-online/docs/handoff.md`, nor the repository's existing unrelated WIP.
