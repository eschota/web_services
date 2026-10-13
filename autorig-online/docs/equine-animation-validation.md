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

## Latest concrete checkpoint

- F15 source preparation PASS: farm job `14581fae0e7040b1acfc`, 77.8 seconds,
  26,584 vertices / 39,973 faces; immutable actionless bundle 3,667,250 bytes.
  Manifest SHA `64523e1e230fa0849ecc519df4a41f5de2cba5f8779978a962e73ac908041e19`;
  anchor map SHA `1a9c0d3d22ad6d02a3c126b66215669b2ab8cb7429797fe4e9ec419ae2049323`.
  This is source preparation, not the customer rig latency or animation acceptance.
- Export subset v5 code ZIP SHA
  `a343f478153abbca9b46f19af3b07be7b81c4ef4ce0d812feb21926a9b5943e0`.
  Earlier smoke failures were corrected by the adjacent console Python, import
  closure and Blender-version compositor detection; keep their failed receipts.
- F15 managed Python3.10 runtime installed. TAPNext++/SAM2/VDA-small checkpoint
  sizes and SHA-256, source Git commits and licenses all verified on F15.
  Existing weights were copied, not newly downloaded; owner GPU was not used.
- New zebra Walk reference: Renderfin `98ab5cae-8d73-4ea2-bbae-bd7f50a8a80b`,
  H3 on f12, 132.2 seconds, video SHA
  `ebe1c36a7b0643bdc0a43efb3498480d59f480551cc6e5f7ec772bc19397256a`.
  DEV media `b415b1de1e9d`, Telegram 7129, scope **reference video only**.
  **Reference accepted**: owner inbox 2456 (`78ec4c2faad1`) at 15:14:26 UTC,
  «Принять ✅». Receipt saved in the project reference folder and cursor acked.
  This never approves the 3D retarget or full34 release.
- Full-runtime fitting code artifact SHA
  `594b6e43ec05e645ecb36b22a7238d3152b924a6184292997a7daa8466459b25`.
  Next: source observations/explicit run-scoped constraints, fit, actual Blender
  apply/export and re-read glTF reference channels. Blender local bone bases
  cannot be called glTF local transforms by simply rotating the quaternions.
- Same-chat continuation/inbox heartbeat `v3-dev-34` is active every five
  minutes, quiet on unchanged/non-actionable state. It must continue these
  existing jobs, never duplicate them, and keep owner verdict scopes separate.

No complete accepted animation library exists yet; all family pointers remain
unpromoted and customer-path integration remains pending.

## Source tracking checkpoint: accepted reference 2456

- Farm observation v1 job `1fbb885ca4884e29b752` failed before inference (5.2 s):
  the old tracking CLI requires 14,000 MiB free GPU memory, impossible on F15's
  8 GB GPU. At pickup only 683 MiB was free. The failed log/result is preserved;
  there are no observations from that attempt.
- An isolated resource adapter now retains the old 14,000 MiB default and
  accepts a range-checked, explicitly run-scoped limit [4,096,24,000]. The F15
  candidate requests 6,144 MiB, sequential fp16/256 TAPNext++, SAM2 tiny, VDA
  small with encoder chunk 1. This is an engineering trial, not a proven memory
  budget. It refuses while customer ComfyUI jobs are queued/running; optional
  idle-cache release only unloads cached models, never cancels a prompt.
- Observation v2 `23c52fd4b2134588992f` refused cache release because customer
  ComfyUI work was active (15.3 s). No inference or cancellation occurred.
- Observation v3 `61f27a5ac3864b60afdb` ended after 306.5 s: customer ComfyUI
  remained busy throughout the bounded 300 s wait; inference never started.
  Preserve the failed receipt. Do not immediately repeat while the same resource
  condition holds. Next heartbeat must verify F15's idle queue first, then use
  a fresh candidate ID and the updated explicit ffprobe path.
  Farm work root:
  `C:/ProgramData/AutoRig/equine-fitting/jobs/zebra-walk-observe-2456-v3`.
  Inputs/receipt pins live in production project `observe-v3/`; full masks and
  depth remain on F15 for fitting; compact diagnostics return through farmjobs.
  Never duplicate a leased job. All three observation attempts are now terminal.
- The accepted five-second video is not automatically declared a cyclic clip.
  Observation uses the six exact immutable source anchors plus dense seeds;
  source identity/visibility and calibrated depth still need real QA.
- `ffprobe` was absent on F15. An existing installed binary is copied into
  its managed project, and the next job explicitly pins its path. No inference
  observations exist yet. Do not confuse queue leasing with a completed stage.
- New Blender apply/export/re-read adapter and a code-only subset are prepared.
  Code ZIP SHA `a1473514e5ba8d45313cb8cc579ceade1f263721e1d98ba53014fb9fdb744819`.
  Actual execution awaits fitted motion. Registry candidate remains pending QA.
- GLB sampler physical right is measured from right-minus-left bind hoof
  anchors; forward requires explicit rig metadata. Missing axes remain pending.
  Weight/sole-band options are range-checked and preserved in each receipt.
- Production contract checks: 29 tests PASS in 0.058 s (5 sampler, 24 motion).
  `equine-motion` CLI is installed and listed live at `/dev/tools.json` with
  options/describe verified. It only produces diagnostics; no customer-path
  activation or automatic library promotion was added.
- Real first-frame geometry preflight on F15: combined correlation **0.825949**
  versus the existing 0.65 threshold (intensity 0.989383, edge 0.626195).
  Actual decoded reference has **141 frames**, 768x448. The earlier Renderfin
  nominal 125-frame setting does not replace decoded-video provenance.
  This CPU check loaded no model and proves alignment, not motion quality.
- Installed DEV CLI evaluated the preserved rejected Walk sample as
  `pass=false`, evidence/action gates `PENDING`. It did not turn fixture PASS
  into animation acceptance.
