# Equine Walk source reconstruction checkpoint

Agent: `Найди handoff по V3`; project: `AutoRig V3 · копытные`.
This is the source-library conveyor work, not a customer-task replacement.
The complete approved full34 library and its customer activation remain pending.

## Owner verdicts and actual artifacts

- Zebra task `d3d37033-08d2-4fd2-94e7-69895481aba4`, tested rig
  `c5ff1c6306858a9e5e8a`, source GLB
  `ace8a3c0adcab0fe33ad24269d3fc4993ee6a1d28f19ab6d5f3b0ef57c2c816c`.
- Original Unity Walk: media `72c401c9d051`, inbox2455: REJECTED.
- Reference video only: media `b415b1de1e9d`, inbox2456: ACCEPTED.
- RAW tracking diagnostic only: media `c2c46ae347c6`, Telegram7208,
  inbox2504 at18:18:02UTC: ACCEPTED. This does not approve depth, the fitted
  animation, another task, or a family release.
- Real Unity source-derived Walk v2: farm capture `96dc090ae7234b7a8764`, f1,
  540frames/60fps, exact candidate override verified in the Unity loaded event;
  candidate SHA `077fdcb871efdc3a06378cd43457d191f8543f86999f90ad2fa8e1c0662f1a89`.
  DEV `912ef8dddb56`, Telegram7212, inbox2507: REJECTED.
  Inbox2508: «ну ты не понимаешь как ходят лошади, переставляя ноги чередуя».
  Treat four-limb alternation as an explicit unmet requirement.

## Tracking preservation and the Windows process defect

Observation v7 `076419b030e645f69a29` failed at wrapper level after19.9s:
the Windows venv redirector PID differs from the actual interpreter PID.
The wrapper raised before controlling its child. The exact own interpreter
was subsequently identified by command line and stopped when customer ComfyUI
work was active; only our process was stopped, the RAW data remained intact.

Preserved RAW scope:
`1e05f37ca01ce8c94f714d046296cf97b283bdb4c394baaade1652e09c0e8720`.
141decodedframes,24actualsource anchors,768x448; NPZ SHA
`ebcea44d44145a051dc30b19a11af661773c39c75ab9d949c6ace1c385e1a67d`.
Tracks/masks remain on F15; no accepted camera-Z depth was produced.
The failed job receipt is not an accepted observation receipt.

The watchdog now uses a fresh actor token, validates either a direct process
or the redirector's actual child, publishes its marker atomically, and stops
the entire own interpreter tree even after a watchdog exception.
Actual F15 CPU-only redirector smoke: launcher7756 / interpreter15392,
own tree stopped PASS, no CUDA imports and no customer process touched.

## Actual fitting and post-retarget measurements

F15 managed root: `C:/ProgramData/AutoRig/equine-fitting`.
The immutable source bundle, anchor map, accepted video and runtime hashes
remain pinned. ffprobe confirms141frames/24fps. All candidates are non-loop;
no gait-cycle crop or retime has been approved.

| Leg | Confident Toe frames | v1 median target error / H | v2 median target error / H | v2 p95 target error / H |
|---|---:|---:|---:|---:|
| LF |137/141|10.44%|3.13%|7.41%|
| RF |51/141|5.34%|1.47%|4.30%|
| LH |123/141|7.78%|1.63%|3.35%|
| RH |116/141|8.10%|1.75%|4.70%|

v1 fixes the root; v2 adds measured torso-driven forward/up root translation.
v2 source solve took321.969s on the farm. This is one-time source fitting,
not customer rig latency. It is not a gait acceptance metric.

Actual Blender4.3 apply/export succeeded. Derived source GLB SHA
`c4f8c0dacad5428db6fb35be9403f299c90d36c205d4a868e8c75a101cb76312`.
Real source and retargeted LBS samples both show7.49–8.95%H penetration and
0.97–2.85H/s maximum planted slide. Physical QA FAIL; thresholds were not relaxed.
Source/target hoof-height differences are at most1.81e-7H; contact counts match.
Retarget maximum rotation difference0.04149degrees and translation5.09e-7units.
Thus the remaining defects originate in source fitting, not GLB serialization.

Actual Blender static rest-scale channels are verified constant before removal.
Horizontal equine bones use declared anatomical up for a stable rest frame;
zero or fully singular axes still fail. Derived GLBs preserve original geometry,
weights/materials/textures and BIN prefix. Sampler respects explicit loop metadata
and Float32 endpoint noise instead of inventing an extra sample.

## Why the far foreleg stood still

The early fitter used four Toe targets and four torso targets, ignoring twelve
other measured limb anchors. After frame55 RF Toe has0/86 confident frames,
but RF Foot has64/86, Leg86/86 and UpLeg86/86. RF Leg motion relative to torso
is92.8x27.1pixels, median confidence0.873; UpLeg confidence0.993.
The own leg has real motion evidence; it must not be mirrored from its partner.

Constraints now retain exact source-bound Foot/Leg/UpLeg tracks independently.
The fitter consumes their own measured targets, retains hidden Toe as PENDING,
and records per-joint provenance and target errors. Smooth robust residuals act
on optimizer residuals only; source points are never clamped or replaced.
LF Toe/Foot tracking also has suspect association on frames15–30; stronger
chain/silhouette consistency remains necessary before declaring source identity
valid throughout the clip.

## Physics candidate and continued work

- Source-plane contact evidence uses measured per-hoof lower envelopes and
  adjacent source-frame speeds, never opposite-leg phases or a gait prior.
- Physics uses real immutable Foot/Toe sole sets, distinct vertex IDs,
  nonpenetration and measured-contact height/velocity constraints. The ground
  is explicitly `SOURCE_GEOMETRY_PLANE_CANDIDATE`, not video-observed ground.
- Bounded sparse temporal windows preserve completed diagnostic checkpoints;
  partial parameters are not an animation and are not accepted automatically.
- Toe-only physics v3 was deliberately stopped after its frame19 checkpoint
  when the ignored limb-chain evidence was identified. Files are preserved in
  `jobs/zebra-walk-plane-fit-v3`; do not resume the superseded process or replay GPU.
- Next candidate must include all measured limb-chain targets and then pass
  real source/export/retarget LBS checks and fresh Unity owner review. Correct
  four-limb alternation is required, separately from reduced target residuals.

Production contract suites passed107top-level +28adapter tests on F15, CPU only;
these are not model acceptance. The Windows registry concurrency containment
race was repaired and tested without weakening symlink/junction protection.
`equine-motion` and `equine-constraint-fit` are registered in `/dev/tools`;
VPS fitting CLI only exposes options/preflight, heavy execute is farm-only.
Source prototypes are not mounted into customer tasks, and family pointers remain
unpromoted. Preserve per-task owner verdicts across the original five-task corpus.

Latest actual run: `jobs/zebra-walk-plane-fit-v5` on F15, code artifact SHA
`ba2bb86cef5b4774d57992a157b960b3f61e879ef959fef9a5266b9ed2c59b72`.
It uses the twelve measured joint targets, robust Toe/joint residuals, sparse
physics windows and the actual managed farm identity guard. Source arrays were
reused; no GPU inference was replayed. The run ended with ERROR at window35–40
because `max_nfev=250` was exhausted. No full clip was written. The completed
frame0–34 prefix survives under that exact farm project directory and locally
as `fit-v5-prefix.json` (SHA411ceb498eed4a61e72fbaf7054b21d8075acef7058aaa3c5ba8800fa6ecbb2e).
Do not replay its GPU stages or restart the whole source blindly.
V4 was stopped with its frame34 checkpoint intact after its coarse dependency
pattern proved too slow. V5 derives exact Jacobian columns from every nonzero
skin influence and its real ancestors, including cross-leg weights. Its
first30frames took73.77s versus349.09s for V4, without changing the objective.
V5 records exact variable order and objective schema; old prefixes are not
silently resumed or accepted.

V6 is an explicit continuation of the attested V5 prefix, not a new observation
or an approved clip. Original objective profile SHA and all data/physics options
remain unchanged. A separate pinned solver-control profile uses500evaluations
and1e-6 termination tolerances, with900s bound; no quality thresholds changed.
Code artifact SHA89bab96683e915e5186a227c34fe6c57384a6a270df4d3e01002281d5f3490b1.
F15 `jobs/zebra-walk-plane-fit-v6`, local execution session16475 is active.
The failed window35–40 passed in68.8s; frame45 started at94.64s. Do not duplicate
the process. Failure checkpoints now preserve best finite failed-window values,
and resumed prefix metrics are recomputed by FK without changing parameters.
Latest farm contract suite:107top-level plus30adapter checks,137PASS.

## Actual continuation and Unity diagnostic, 2026-10-13 20:46 UTC

The active-V6 statement above is superseded. V6 ended ERROR at window45–50;
its frame0–44 prefix is preserved. Exact finite-difference audit found zero
undeclared Jacobian dependencies. The input constraints included physically
inconsistent Toe/Foot pairs. The new source-eligibility tool measures actual
immutable weighted-anchor reach, adjacent own-chain support and pair jumps;
it rejects targets without replacing points, mirroring or synthesizing contact.

V7, objective `autorig.equine-window-objective/3-source-eligibility-physics-sparsity`,
used the retained RAW data and ended ERROR at window100–105 after436.156s,
max500evaluations, cost0.36004336. Completed frame0–99 parameters are preserved
in `jobs/zebra-walk-plane-fit-v7/walk-fitted-animation.json.progress.json`.
An exact window100 audit has709rows/90variables and zero unexpected derivatives.
Hoof/joint/torso costs dominate; contact/velocity costs are only9.6e-6.
A same-objective retry was stopped through its own PID1492 after confirming
that solver iterations cannot repair the incorrect root/limb decomposition.
No new suffix output was accepted. No GPU stages were replayed.

The verified prefix exporter replayed FK without optimization into a fresh
explicit `DIAGNOSTIC_PARTIAL_REFERENCE_NOT_LIBRARY` motion (100of141frames,
loopfalse). Motion SHA d3eec1d3e00596ef20c908f4198720b0d790809757590a99169106d5ac09e38b.
Actual Blender apply/export, source GLTF re-read, target retarget and LBS sample
all completed on F15. Target candidate SHA
194646fd85aa553dfb9eadc0e25c1d9f9a98a154ebc5a137c0e89a16e88049f2.
All32bones retain real100samples/4.125s. Originals remain unchanged.

Same exact source frames0–99 at24fps, inferred bind-floor diagnostic only:

| Leg | Before penetration %H | After penetration %H | Before slide H/s | After slide H/s |
|---|---:|---:|---:|---:|
| LF |7.60|1.46|1.59|2.73|
| RF |6.96|1.49|0.97|0.92|
| LH |7.78|1.04|1.22|1.16|
| RH |8.14|0.67|1.28|2.20|

Reduced penetration is not gait acceptance: LF/RH vertical swing is compressed,
and output proximity to an inferred floor cannot establish video contact.
Source measured-contact coverage is poor; hidden frames stay pending.
Loop/four-beat-loop gates are not an acceptance metric for a partial one-shot.
Correct four-limb alternation and actual grounded stance remain unproven.

Fresh Unity540frames/60fps capture on F1, farmjob c03977776e814f4dac85,
exit0, exact candidate override/Walk verified in loaded-event log.
DEV media1243df8284b0/Telegram7279 is explicitly partial100of141, QAFAIL;
owner verdict pending (own inbox cursor2508). It is not a new accepted Walk.

F15 suite-v14:107top-level+36adapter=143contractchecksPASS. ZIP SHA
90f95df78a45abad4e941ccb37d0ebab47988ee74f6e6d6d2dbe6a7ea4236247.
Tested fitter snapshot installed atomically into DEV CLI (sourceSHA
ba38133446d28b3127ee3e1e096810aa625e32d75a8cd522ca141556b86244cb);
VPS options/preflight only, customer path/library still inactive.

Next: new objective with same-frame measured limb-to-torso vertical residual,
eligible own anchors only, geometry nonpenetration one-sided. It needs a fresh
objective schema/fit, not an incompatible prefix resume. Then real export/LBS,
fresh Unity per-task verdict and the remaining full34/category corpus gates.

V8 started20:47UTC on F15 with fresh objective
`autorig.equine-window-objective/4-source-relative-vertical-eligibility-physics`.
Actual actorPID5692, launcher16092, foregroundSSHsession59410; follow this process,
do not duplicate it. Source/constraints/contact/eligibility inputs are preserved,
no GPU work. ProfileSHA cc01647165a99292e057961e2fb8791db4968fc7a94e41d20f6d1a15b40d5a96,
fit-jobSHA 291ec6a93c82a2cf40004195aea4855acebeb60511fed50140987c83acf5dd67.
Solver max500/tols1e-6/budget900; QA thresholds unchanged.
Latest suite-v15 production144PASS (107+37), including real root-translation
invariance/missing-torso admission checks. ZIP SHA
e196ff90a449037a1b1cba6ecc3db2b4aa0f7edf42b4c5cc451d1da66df025d8;
fitterSHA 4129ec02e45c2a155154da1e57723cacbb13d4053afe628c79e96ddefb1d3043.
This exact tested fitter is installed in DEV CLI; `equine-prefix-export` is also
registered with a describe contract. Both remain diagnostics/farm-only execute.
