# Animal animation development, recovered 5 September 2026

## Scope and recovered decisions

Recovered Claude Code session: `a2680381-3736-4233-a3f4-eb2b8b332c7b`,
1–2 September 2026, project `R:\autorig`. Its last preserved source commit is
`33b26345f0bd29626e8637057f0930e72917ecc1` on `codex/rig-page-20260901`;
the remote branch was verified before continuation. The old worktree
`R:\autorig_rig_page` no longer exists. Development continues inside
`R:\autorig\work\animal-animation-20260905`.

The owner wants horses first, without a rider, with the basic actions needed
for an RPG animal controller. Build the reusable animation library once and
retarget it to new animal rigs. Do not expose expensive per-user fitting in
the website yet. Use owned video/generated references and existing sources;
do not assume a paid animation pack is available. Output animation is 30 FPS,
with action-specific timing, rather than a universal 49-frame budget.

Development notifications are sent to `https://autorig.online/dev/`, under
`AnimalRig-20260905`, project `AutoRig Animal Animation`. The API contract was
read from `/dev/api/spec`. Notifications are evidence, not a substitute for
motion quality. No earlier session or inbox was cleared.

## Verified problems and fixes

1. The real horse profile omitted **40 segmented twist bones**. Their weights
   became neutral body colour, leaving gaps in all four legs. The explicit
   limb allowlists now include those bones. Optional `body_bones` declares the
   remainder; when present, every deform bone must belong to exactly one
   declared region. Both horse profiles enable that coverage check.
   Re-derivation from the immutable real bundle assigns **31,676** limb faces,
   versus **24,952** previously: **6,724** faces recovered. The source geometry,
   weights and immutable bundle are preserved.

2. Colour on the rump is a separate upstream rig defect. The real horse has
   hind chain lengths **0.771512** and **1.626674** in armature-local units,
   a **52.571%** relative difference. Fore chain difference is **7.060%**.
   The canonical source has **0%** paired-chain difference. The new morphology
   preflight measures full chains and individual segments and rejects this
   real source before video generation. The current 20% threshold is an
   explicit engineering gate; passing it is not production approval.

3. The clip runner referred to the deleted worktree and copied from a fixed
   Comfy output directory. It now resolves code/specs relative to its checkout,
   requires an explicit output directory and verified source bundle, downloads
   its exact output via Comfy `/view`, rejects `node_errors`, and verifies the
   decoded frame count, dimensions and 30 FPS. The job ID, source hash, graph
   and lifecycle state persist in `run.json`/`workflow.json` for recovery.
   Existing Comfy jobs are neither cancelled nor replaced.

4. `game_timing.py` separates LTX latent sample restrictions from skeletal
   sampling. For example, `run` uses 41 generated samples and 21 skeletal
   samples; 21 need not satisfy 8n+1. Samples include the endpoint, so the
   33-sample walk contains 32 loop intervals and lasts **32/30 seconds**.
   These are editable authoring defaults, not measurements of animal cadence.
   The resampler uses shortest-path quaternion SLERP, preserves one-shot final
   poses, reports seams without hiding them, and removes old approval/UUID
   metadata. The result must be reviewed as a new candidate.

5. The previous session ended after a GPU crash while LTX and depth inference
   overlapped. The LTX runner and CUDA tracking CLI now share a project lease.
   CUDA tracking checks free physical VRAM before model loading. This lease
   coordinates this pipeline only; other applications still require an idle
   GPU window. A crashed lease is not removed based on age.

## Commands

Run from the checkout root. All examples keep new artifacts in
`autorig-online/work/animal-pilot`, which is ignored by Git.

```powershell
$env:PYTHONPATH = "$PWD\autorig-online\tools"
$python = 'R:\ComfyUI-data\autorig-fitting\runtimes\venv-py310-cu128\Scripts\python.exe'

& $python -m animation_fitting.audit_horse_rest_rig `
  --bundle <immutable-horse-bundle> `
  --output autorig-online/work/animal-pilot/rig-audit.json

& $python autorig-online/tools/animation_fitting/workflows/run_ltx_clip.py `
  --bundle <immutable-horse-bundle> --image <bundle-reference_ltx_semantic.png> `
  --action walk_forward --out-name horse_walk `
  --output-dir autorig-online/work/animal-pilot/candidates/horse_walk `
  --dry-run

# Remove --dry-run only for an actual, sequential Comfy render.
# Generation output is still pending gait QA, never an approved animation.

# After a client timeout, collect the same prompt ID; never submit a duplicate.
& $python autorig-online/tools/animation_fitting/workflows/resume_ltx_clip.py `
  --run-dir autorig-online/work/animal-pilot/candidates/horse_walk

& $python -m animation_fitting.game_timing `
  --clip <complete-cycle-three-animation-clip.json> --action walk_forward `
  --output-dir autorig-online/work/animal-pilot/retimed/walk_forward
```

Both the generator and tracking CLI accept an explicit shared `--gpu-lock`
path when several checkouts use one GPU. On the tracking CLI it is a global
option, preceding `observe`. A held lease or insufficient free VRAM must be
resolved before loading a second model; do not terminate someone else's job.

The existing direct GLB packager accepts the resulting uniformly sampled
Three clips after fresh review. The legacy plan compiler and FBX/release
packager still enforce the original v1 taxonomy frame profiles: they need an
explicit versioned timing contract before the new budgets can be activated
through that route. No production library was activated by this change.

## Remaining work to a usable controller library

* Accept one actual four-leg walk reference with stable identity, valid
  contacts and an extractable complete cycle. Video generation completion is
  not gait acceptance.
* Finish and validate the anatomical fitting target. The recovered
  `horse_arp_deform_v1` still declares `joint_limit_profile_missing` and
  `linearized_bbone_target_requires_manual_acceptance`; do not bypass these
  with unbounded joints and call that production quality.
* Track, fit, bake and continuously inspect a real walk on the canonical
  mesh. Check four hoof contacts, sliding, joint limits, deformation and
  both pose and velocity at the loop seam.
* Complete idle/walk/backward/trot/run/sprint, turns/brake, jump phases,
  eating/rest, hits/attacks/death/get-up/vocalization. Correct entry/exit poses
  are necessary for sleep, eating and get-up; a list of 30 names alone does
  not provide those transitions. Airborne clips cannot be conditioned on a
  standing reference merely because both use the same rig.
* Export GLB plus FBX and re-import the actual files. Supply semantic action
  IDs, timing, root-motion policy, measured contact events and transition
  pose compatibility for the downstream controller.
* Retarget to distinct horse morphologies, fix the real rig's asymmetric
  joints/skin deformation, and verify continuous locomotion and action
  transitions. Only then publish the reusable library and proceed to other
  species with their own anatomy and gait profiles.

Full gameplay readiness is still unproven. This continuation repairs source
semantics and the authoring toolchain; it does not claim a finished animal
controller or a complete animation library.

## Executed pilot and continuation state

Current checkpoint: see `quadruped-grounded-authoring.md`, especially the
forelimb placement and centered support stance section. Claude context recovery
is complete. The production bind repair passed the owned real-model task;
an offline shoulder/elbow prototype has a user-accepted full-mesh video.
P3 separated the previously collapsed hind hips and tested new stifle placement.
P4b then restored the neck/head path, and P4c/P4d corrected ear placement and
limited its weights to the actual ears with a smooth transition. P4d is now
the full-native reference; keep earlier failed geometry and mask experiments
as rejected evidence. Its ten draft actions (including run and sprint) pass
numerical reduction, actual Blender four-weight fidelity and independent
ear-locality checks, and all twenty actual GLB/FBX reimports are complete.
Remaining anatomy/gait approval, contact polish and gameplay actions/transitions
remain explicit gates. Passing fidelity alone must not substitute for
anatomical validation.
Do not restart completed ear-mask searches or reuse earlier P3 weight results.
The subsequent P5 contact-body candidate replaces only run/sprint and retains
eight other P4d clips byte-identically. It has passed fresh four-weight,
actual Blender, independent locality and twenty reimport checks. See
`quadruped-contact-body-motion.md`. Its comparison and self-contained GLB have
been delivered. Continue with missing jump/landing/action transitions rather
than repeating completed P4d/P5 validation without a new change.
Older LTX notes below are historical evidence, not the next task to restart.

* Blender 5.2.1 built `anatomical-reference/anatomical_rig.blend`: 52 bones
  including the master root, 51 deform bones, 344 vertices, at most 4 weights,
  zero actions/constraints/drivers. The build is explicitly reference-only.
* Native preset inspection found `Horse_default` at frame 0 and
  `Horse_gallop` at frames 0–18. An inspection export produced a 127,840-byte
  GLB with 51 joints and one baked animation spanning 0.6 seconds at 30 FPS.
  That is a baseline export, not a newly authored complete library.
* The initial LTX run on the existing 8188 worker was cooperatively interrupted
  using only its prompt ID after Windows reported about 3.8 GB shared GPU
  memory and progress became very slow. Restarting that pre-existing worker
  was denied by automatic approval review; no alternative method was used to
  restart it. Instead, a separate worker was created on 8189, with private
  user/input/output/temp directories **and an explicit private database URL**
  inside `work/animal-pilot/comfy-private`. `--user-directory` alone did not
  isolate Comfy's default database. Launch arguments are saved in `launch.json`.
* The private worker uses `--reserve-vram 6 --lowvram --disable-smart-memory`.
  Observed shared GPU memory was about 0.48 GB during sampling. Candidate
  `horse_walk_v11_reserved`, prompt `183b3b92-6797-450a-acf8-bccba8afbaca`,
  finished in **10 minutes 16 seconds**: 65 decoded frames, 768×448, 30/1 FPS,
  SHA-256 `6747bea4769475b0c2fe15c18f0951546a0dfc92cc23371f999708e05d4de3d1`.
* The candidate is **REWORK**. The automatic period estimator selected source
  frames 12–55.773 and introduced a 41.38 px hoof seam. Checking the entire
  guided source span independently showed that this large seam is a slicing
  problem: raw full-span hoof closure is at most 2.15 px, and at most 1.683 px
  in the explicit 33-sample retime. Even that complete span fails the gait
  checks: irregular detected footfall sequence, low contact duty factors,
  nonuniform/contradictory contact drift, and body endpoint mismatch 12.157 px.
  Do not confuse the slicing defect with the motion defects, or reduce gates
  merely to obtain an accepted result.
* Evidence is in `gait-v11/metrics.json`, `full-span-metrics.json`, phase sheets
  and `horse-walk-rejected-review.mp4` (six repeats, 32 unique frames per loop).
  The record state is `rejected_by_gait_qa`. No tracking, fitting, approval or
  production library activation followed this rejection. Private model caches
  were released after the queue became idle.
* A thread heartbeat named **Animal Rig — разработка и DEV**, ID
  `animal-rig-dev`, continues every 15 minutes. Notify only on meaningful
  changes through DEV. The next technical work is guided-cycle selection and
  reliable gait/contact control, plus anatomical joint-limit/deformation
  validation. Do not generate random variants of a rejected motion endlessly.
