# V3 hoofed animation conveyor: owner validation

Owner work order: 2026-10-13, this Codex session (DEV identity `Найди handoff по V3`).

Use existing server tasks only. Generate references once per anatomical family;
extract their real motion, retarget automatically, and validate each task with
the owner through `/dev`. A task acceptance does not accept another task, and a
reference-video acceptance does not accept its retargeted animation.

## Scope

The first engineering milestone is the 14-action locomotion/behavior core:
idle, walk, trot, canter, gallop, start, stop, turn left, turn right, backward,
jump, rear, look around, tail.

The owner explicitly confirmed the full game set, including attacks,
directional hit reactions, falling, death and getting up. The project defines
`animal-base-30-v1` in
`tools/animation_fitting/compile_animal_library_plan.py` in the September animal
animation worktree. The full target retains that 30-action controller set and
adds equine canter, locomotion start, rear, and tail: 34 actions.
The 14-action core alone is not the complete full-game library.

Families have separate releases and validation corpora: equid, cervid,
camelid, bovid, suid. Passing a horse does not authorize a camel/deer library.

## Initial existing equid corpus

| Task | Existing MT run | Role |
|---|---|---|
| `31cc3ff3-1526-41e2-bcf4-aaac56c38015` | `028d1acdb7187c532921` | White horse |
| `d3d37033-08d2-4fd2-94e7-69895481aba4` | `c5ff1c6306858a9e5e8a` | Zebra |
| `a742491a-981e-43aa-bfcb-3b465be8ff91` | `3ef6defe5e2555ce4277` | Horse with tack |
| `ebc474ba-fb5c-4d3e-b0a6-4fa15c655fb4` | `c33c0d6f735cfb9e1c8f` | Saddled horse |
| `74f7cd37-1350-4ac2-985d-885503589e17` | `5b447b5d03cd66f8fa6a` | Sitting horse / folded contacts |

Re-read the live task/run/rig hash before each job: rerig-on-open can replace
the version. Original customer inputs and outputs stay untouched. A candidate
preview is a separate version with an undo path, not hand-animation of a task.

## Acceptance

1. Immutable reference/candidate bytes, source task/run and retarget code hashes.
2. Actual observed four-hoof identities; no half-cycle synthetic far-leg proof.
3. Post-retarget ground contacts, slide, penetration, gait order, aerial phase,
   loop seams, root direction/yaw, and action-specific head/tail measurements.
4. Deformation QA and complete action coverage on each corpus task.
5. Real Unity viewer media for each task, pinned to the candidate/rig hashes.
6. Separate canonical DEV owner verdicts on every presented task/version.
7. Atomic family release promotion only after the complete target passes and
   every mandatory task is accepted. New tasks resolve that immutable release
   and run their own retarget QA. Generation completion alone never promotes.

## Current engineering checkpoint

- Existing side-view library has no owner-approval boundary. Legacy entries
  with absent quality currently attach; new generators overwrite canonical
  entries. Neither is acceptable for the new accepted release mechanism.
- The existing gaitgate deployment is owned by Fable and still running. Do not
  start a duplicate gate or generation job over its shared files.
- New isolated modules live under `autorig-online/deploy/equines/`. They are not
  yet mounted in the rig path; do not describe them as production activation.
- On production, `test_equine_motion_contract` passed 14 tests in 0.019 s and
  `test_equine_library_release` passed 14 tests in 0.303 s.
  These test the QA contract, not real model or owner acceptance.
- A source-bound live GLB sampler, DEV registry, and farm engine capture are in
  progress. No new full animation library has been accepted or promoted.
- Runtime/model discovery and adaptation of the existing animation-fitting
  stack are required for full 3D extraction. The current sagittal tracker
  cannot prove turns, head yaw or the four-leg phases of canter/gallop.

Public endpoint: https://autorig.online/dev. Keep private models, secrets and
raw customer data out of this document and Git. Raw test/media receipts remain
in `.work/equines-20261013/` and the corresponding production project folder.

## Owner rejection: zebra Walk baseline

DEV media `72c401c9d051`, Telegram 7123, real farm Unity capture
`c12269f81a594f52a1ae`: the existing zebra Walk (task rig `.7`, server `.8`).
At 2026-10-13 13:11:35 UTC the owner replied, inbox 2455:
«это полностью некорректная ходьба или анимация.»

Status: **REJECTED**, not accepted baseline and never library approval. Preserve
the video/rig/library hashes and include this case in the semantic-motion
regression corpus. The new clip must improve actual four-hoof contacts, phase
order and motion, then receive a new per-task owner verdict. Numeric deformation
alone (the earlier 10/10) does not prove that a walk is anatomically correct.
