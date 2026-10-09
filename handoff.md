# AutoRig development — start here after context compression

Updated: 2026-10-09, Asia/Novosibirsk. Owner-required persistent checkpoint.

The primary work is GPU erosion → anatomical bones → skinning. The owner has
confirmed migrating humanoids to V3, including a non-destructive calibration
T-pose and adaptation of animation retargeting. The canonical checkpoint links
the new numbered migration plan. MT/viewer/diorama presentation is complementary,
not a substitute for a working skin. F5 stays isolated; this is not yet a
production-ready full humanoid rig or a production-default switch.

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
