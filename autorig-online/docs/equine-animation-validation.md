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

## Observation and integration checkpoint, 2026-10-13 17:05 UTC

- v4 `afb07ebb5a3f459d86f1` reached an idle F15 but refused the 6,144 MiB
  trial with 4,873 MiB free. v5 `3aad50ce8af140b59391` actually completed
  TAPNext++ tracking, SAM2 masks and VDA-small inference in 178.7 s. Peak CUDA
  allocated 4,093.28 MiB, reserved 5,186 MiB. This is measured source-job cost,
  not a customer rig latency or a general memory guarantee.
- v5 calibration **FAIL**: absolute Spearman 0.60206; median error 2.71% H;
  p95 error 19.51% H. The camera-Z thresholds were not relaxed and this was not
  declared valid 3D observations. The old runtime wrote raw arrays only after
  calibration PASS, so those completed model outputs were not retained.
- v6 `a62874c4ba944b839f2f` yielded before inference when a customer render
  arrived during the idle-cache admission check. All observation jobs are now
  terminal; no observation GPU job is running.
- Raw measurement preservation/reuse and a customer-priority GPU watchdog are
  prepared. The watchdog terminates only its own actor, verifies its PID before
  releasing its lease, and retains completed stages. Raw measurements remain
  explicitly uncalibrated/unapproved. Candidate v7 spec is prepared in project
  `.work`, **not enqueued**; check actual jobs and farm state before dispatch.
- Added strict observed-3D adapter, source sagittal-limb constraints, and actual
  derived-GLB writer. Sagittal constraints are candidate assumptions for linear
  locomotion; hidden frames are not synthesized, and no ground/camera confidence
  or accepted depth is invented. Turns/combat are outside that adapter's scope.
- Production lab suites: 28 tests PASS, plus Python compilation checks. Actual
  F15 immutable bounded-bundle loader smoke PASS (32 bones), originals unchanged.
  The limits in that smoke were tool-test values, not anatomical/default approval.
- Source glTF ancestor TRS is composed before retargeting; the writer preserves
  protected target structures and original BIN prefix. Real fitted/exported Walk
  and per-task Unity QA remain outstanding; these tools are not customer-mounted.
- Work was interrupted by the owner's customer task incident. See
  `v3-task-completion-20261013.md`: Telegram callbacks/statistics repaired in
  production, but the five customer rigs still need their genuine skin defects
  repaired. Diagnostic media 7155 does not approve a rig or the equid library.

## Measured reconstruction continuation, 2026-10-13 19:20 UTC

Canonical detailed checkpoint: `equine-walk-source-physics-20261013.md`.
RAW v7 data were retained despite a Windows venv watchdog PID failure; the
watchdog was repaired and verified with a real own child on F15 without CUDA.
Owner accepted only the RAW diagnostic (inbox2504). Actual source-derived Walk
v2 was exported, retargeted and captured in Unity on f1: DEV7212/media912ef8dddb56.
Owner rejected it in inbox2507 and explicitly required proper limb alternation
in inbox2508. Source fitting, not retarget serialization, caused its physics FAIL.
The early fitter ignored twelve actual limb tracks; RF knee/upper-leg remained
confident on all86frames after its Toe confidence collapsed. The next source
fit uses each leg's own Foot/Leg/UpLeg measurements and explicit physical
constraints. Hidden Toe positions remain PENDING, no mirroring or gait prior
is labelled observation. Farm run `zebra-walk-plane-fit-v5` ended at window35–40
with max evaluations exceeded; frame0–34 prefix is preserved. No fit process is
active. Continue only by explicit pinned resume, without GPU replay. Production
F15 contract suites135PASS are not model
acceptance. No full34 family pointer or customer-path activation has occurred.

V6 is now active: explicit verified continuation of the V5 frame0–34 prefix,
preserving the objective profile and changing only pinned solver controls.
Actual window35–40 passed after68.8s; next frame45 after94.64s. F15 project
`zebra-walk-plane-fit-v6`, local exec session16475. Full137farm contract checks
pass, but real complete motion/export/retarget/owner acceptance are still pending.

### Superseding checkpoint, 2026-10-13 20:46 UTC

V6 is terminal ERROR45–50. V7 source-eligibility objective completed100frames,
then ERROR100–105. Actual100/141 diagnostic prefix was FK-replayed, exported in
Blender, retargeted, LBS-sampled and captured in Unity on F1. DEV7279/media
1243df8284b0 is explicitly partial/QAFAIL; owner verdict is pending.
Penetration improved on the same100frames, but LF/RH sliding worsened and
four-leg gait remains unproven. A source-backed limb-to-torso vertical objective
is required; a changed objective cannot resume the old prefix. Detailed hashes,
before/after metrics, exact terminal jobs and next steps live in
`equine-walk-source-physics-20261013.md`. Latest farm contract suite143PASS does
not approve motion. Full34/category-library/customer activation remains pending.
Fresh V8 objective4 is now running on F15 (actor5692, launcher16092,
foregroundSSHsession59410); source/RAW stages are reused. Latest production
contracts144PASS. Follow that process without duplicating it; exact pins are in
the detailed checkpoint. A running fit is not an accepted clip.

V8 is now terminal:141frames/376.985s, actual export/retarget/LBS done but QAFAIL.
F13 Unity quit crashed after encode; F1 repeat passed and full diagnostic video
DEV7283/media4aa8682e9d4a was delivered. Both task videos remain pending owner
verdicts. Camera audit found18–51px median Toe reprojection errors, so the next
fresh source objective fits unchanged RAWpixels through the immutable camera
instead of treating ray-plane intersections as measured metric-depth targets.
V9 is under implementation, not dispatched. Viewer cleanup is a separate cached
author fix; no customer-path/library activation has occurred.
