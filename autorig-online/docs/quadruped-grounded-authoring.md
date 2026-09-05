# Contact-controlled quadruped motion authoring

This is one component of the full animal auto-rig/animation pipeline. It
currently authors eight horse candidates: five idle behaviours, forward walk,
backward movement and trot. It does not establish a complete production
library, arbitrary-animal rigging, action transitions, or real-mesh quality.

## Executable path

1. `blender_quadruped_bridge.py export-rig` records an actionless compact
   skeleton, mesh topology and normalized skin weights. It rejects hidden
   control constraints and non-linear B-Bone deformation.
2. `author_quadruped_motion.py` uses an explicit gameplay profile to create
   sole trajectories, contact phases, body motion and bounded limb poses.
   It solves three-link sagittal IK exactly by collapsing the distal pair
   into a two-link triangle. A one-dimensional bounded posture selection
   preserves a smooth distal-bend profile instead of switching between
   competing S-shaped leg configurations.
3. `blender_quadruped_bridge.py apply` creates real Blender actions and
   independently measures evaluated skinned vertices at every key and
   half-frame, then exports one multi-clip GLB and one FBX per action.
4. `verify_quadruped_exports.py` re-imports the actual files and compares their
   skinned surfaces against Blender source at every key and half-frame. It
   excludes importer-created custom bone shapes, not animal geometry.
5. `render_quadruped_preview.py` renders continuous movement over a fixed
   half-metre ground grid. Camera tracking is separate from actor travel.
   Output paths are resolved absolutely before loading any Blender file.

The reference skeleton has 51 deform bones plus a motion root. The generator
uses profile-declared chains and body bones; it does not infer bone semantics
from name fragments. Joint bounds are finite engineering authoring limits,
not veterinary range claims or an approval of the complete anatomical fitter.

## Timing and contacts

All timelines are 30 FPS. A 33-sample walk includes its endpoint and lasts
32/30 seconds; previews use 32 unique frames per loop. In-place clips include
a reference speed so the downstream controller can supply actor travel.
`--root-motion` supplies that translation in the root instead.

Stance hoof velocity is linear and compensates actor travel. Horizontal swing
uses a quintic curve with matching velocity and acceleration at the contacts.
Vertical lift has continuous velocity and early clearance to avoid
interpolation penetration immediately after takeoff. Blender measurement is
necessary because exact key poses alone do not prove clean intermediate poses.

Forward walk uses four separate phases; trot uses diagonal pairs. Horse
backward movement uses diagonal pairs without aerial suspension, following
the [FEI description of rein back](https://www.fei.org/stories/lifestyle/teach-me/why-and-how-you-should-teach-rein-back-horses).
These phase rules do not by themselves prove natural visual animation.

Forelimb footholds retain the original anatomical neutral stance. The scapula
provides additional reach. This replaced the earlier hip-centred foothold
offset after the owner observed an excessive forward lean in the front legs.
Small, bounded whole-body height calibration accommodates segment proportions
without changing the requested footholds or removing joint limits.

## Current evidence, 5 September 2026

Artifacts are in the ignored `autorig-online/work/animal-pilot/gameplay` folder:

* `horse-rig.json`: source blueprint, exact source .blend SHA pinned.
* `authored-v8`: eight generated motion candidates with contact schedules,
  reference speed, source/profile hashes, local transforms and mesh QA.
* `exports-v8`: real .blend, GLB, eight FBX files and reports.
* `exports-v8/reimport-qa.json`: **16 format/action checks passed**, comparing
  every key and half-frame. Maximum surface deviation: **0.00003242 m**.
* Blender stance-point error: walk **0.418 mm**, backward **0.154 mm**, trot
  **2.961 mm**. Worst intermediate mesh penetration is **3.291 mm** for trot;
  it remains recorded and must not be concealed by perfect keyframe metrics.
* Nine independent synthetic tests cover contact velocity continuity,
  deformed sole planting, finite joint bounds, scale adaptation, diagonal
  backward contact phases and root motion with rotated root-rest axes.
* A four-cycle walk preview was sent to DEV. The owner's foreleg feedback led
  to the scapula/neutral-stance correction; acceptance on a full mesh remains
  outstanding.

None of these artifacts carries production approval. The real anatomical
fitter's missing joint-limit/deformation acceptance is not bypassed: this is a
separate constrained authoring candidate path, not an unbounded video fit.

## Full-mesh test and current blocker

An existing owned Renderfin `horse-qa` generation was recovered:
`863c06af-99ad-4896-8224-0835043c865a`. Its 11,148,752-byte GLB was submitted
through the authenticated production UI, which recognized Horse and started
task `3c9cdfb0-a1fa-4d5a-b2b9-0321f5dd467d` on F1. Converter GUID:
`6b122b9d-7f94-438a-b2d4-619a415daf98`.

The real run produced a Stage 2.2 semantic skeleton, then failed with
`Animal rigging watchdog timed out during ARP/VHDS bind after 900s`.
The last matching global addon log is Stage 3 safety PASS at 13:45:02, with no
`ARP bind START`. That log line is emitted only after `fit_external_arp_rig`
returns. The identical 900-second message can be emitted by the independent
parent watchdog, so it does **not** prove that Blender's timer remained live.
Do not patch the 300-second deferred timer as though it were the proven cause.

Investigate the synchronous Stage 3 path (preset append/alignment/reference
placement, `arp.match_to_rig`, and `_run_arp_bind` entry) with retained process
stacks/phase evidence. The full mesh has not received these authored clips yet.
No duplicate retry of the failed public task has been submitted.

### Queued replay and confirmed bind hotspot

The converter diagnostic work was independently reviewed and merged into
`eschota/autorig.online` main at `acf2c33dacefb977626452c0fcdb43169774707e`.
Before deployment, five drifted files on F1/F2/F13 and the distinct F11 webserver
were collected read-only. Six already committed Auto-Rig Go repairs were merged
with current main; the actual three-way merge preserved newer maintenance and
status/control protections. The combined relevant suite passed 77 tests and
12 subtests. No licensed ARP source was changed.

The standard artifact rollout confirmed the exact boot commit on F1, F11, F13
and F2; F7 was unavailable and unchanged. No task IDs were interrupted and no
rollback was needed. Stage/apply durations were F1 2.841/18.736 s, F11
3.647/15.174 s, F13 3.105/18.397 s, and F2 3.156/17.242 s. Artifact SHA-256:
`3ebca1057ed90823b3a66e3f05078d0f6e569dcc9bc6e777d7e919fe789e2c6d`
for F1/F13/F2 and
`fbc7dd6bb182d29932968e63c8453c289a208024ccdbdc25cfa3a25066d2a58f`
for F11. Full reports remain in the converter diagnostic worktree's
`work/farm-rollout` folder.

One owned replay was submitted through the normal F1 queue after preparing a
new isolated service task folder and verifying the source SHA-256:
`f11d76873942371c7c5bd2a94693fc3327f11e830a5e71dc3380514d0662995a`.
Converter GUID `2c139ec9-bb52-474c-8054-f5b86e6bf2e3`, worker task
`83416bca-50b6-46bb-9ca5-cc0a093ae6aa`; submission identity and exact payload
are retained in `gameplay/real-bay/probe-replay.json`. Do not duplicate it.

The internal probe's global tracing distorted timing (Remesh 197 seconds),
and its Python 3.13.9 periodic stack file stopped after a malformed 191-byte
dump. The Blender 5.2 smoke therefore did not establish compatibility with the
actual Blender 5.1 runtime. Do not use that probe on further F1 tasks. External
`py-spy 0.4.2`, installed only under runtime `work/animal-bind-probe`, provided
the useful stacks without changing the target process's Python code.

A first sample in `match_to_rig` was normal progress: retained markers show it
finished in about two seconds. Repeated later external samples instead stayed
at licensed ARP `auto_rig.py:20834`, `bpy.ops.mesh.separate(type='LOOSE')`, inside
the HEAT_MAP binding branch with `arp_bind_split` enabled. Our bind launcher
does not explicitly disable that inherited split option.

Independent read-only connectivity audit of the exact input found **119,996
vertices, 40,000 triangles, and 39,998 indexed components**. Exact coincident
position welding yields **20,002 vertices and one component**. Thus loose-part
separation attempts to create almost 40,000 objects. This is the confirmed
hotspot; limb alignment and the deferred bind timer are not supported causes.

The fix in development is an exact-weld skinning proxy with split disabled,
followed by validated weight transfer to the untouched render mesh. Original
vertex identity, UV loops, normals, materials and object identity must remain.
Binding success and visual quality still require a fresh normal-queue replay.

### Fix deployed and real-mesh motion reviewed

Converter main `2c66008169fcedc363b5d5cb884c08133eb15fb4` contains the
exact-weld bind fix and removes the intrusive embedded probe. The normal
artifact rollout verified F1/F11/F13/F2; F7 stayed unavailable. Artifact:
`00716089639adb52ae0104b263b2d4c179f8b0837e5318db96ef7d731cd19441`.
No tasks were interrupted and no rollback occurred. Stage/apply times: F1
2.396/9.959 s, F11 3.072/10.949 s, F13 2.741/11.689 s, F2 2.754/11.937 s.
Four real Blender synthetic tests and 47 relevant pipeline/GLB tests passed.

The fresh owned F1 task `5c8ebd10-2c01-4494-ba08-8fb8c047b553`, GUID
`d03b7019-a6f6-4053-a7b0-864546f0e9a2`, reached **Completed** with no error.
Primary Stage 3 completed in **26.60 seconds**, returning
`FINISHED,EXACT_WELD_PROXY_TRANSFER_DONE`. All 119,996 original vertices received
finite normalized deform weights in 91 groups; zero vertices were uncovered,
and the proxy was removed. The bounded Stage-3 report and exact source snapshots
are in `gameplay/real-bay/d03b7019-stage3`. This proves the bind repair, not the
quality of every fitted joint or animation.

Primary Blend and GLB were downloaded and SHA-256 checked against F1:

* `artifacts-2c66008/rigged.blend`: 104,327,994 bytes,
  `6f00c5cb5cb87e2a26178d12ea8e4dcee225c6b61254d8e31fe8439d9784cc41`.
* `artifacts-2c66008/all_animations.glb`: 12,243,120 bytes,
  `7138c216497346c346e85dd763c3e475915c17066a0100bbb5d8f69d69035b50`.

The bound source contains 344 total bones, 91 deform bones, and up to seven
influences per vertex. Offline authoring preparation creates a separate
four-influence linearized reference. Native gallop comparisons record maximum
surface deviations of 22.23 mm for influence reduction and 29.34 mm after
combined B-Bone linearization. These are limitations, not acceptance evidence;
the original bound source remains unchanged. The compact reference is 92 bones
including the motion root and keeps fitting/quality approval false.

The generated forelimb rest angles also exposed an authoring bug: bend direction
was inferred from the rest angle's sign. It now follows the explicit fore/hind
role. An opt-in bounded rest-posture projection records corrections and keeps
the original joint limits enforced for every authored pose; default profiles
still reject out-of-limit rest chains. Eleven authoring tests, and 26 including
the timing contract, pass. The real candidate uses a 10-degree projection cap
and a shorter walk stride of 0.3 hip-height after the original 0.5 and 0.4
recipes proved unreachable. No joint limits or contact gates were widened.

`real-bay/authored-v3` and `exports-v3` contain the first full-mesh walk candidate.
Blender key/half-frame evaluation measured maximum hoof error **0.491 mm** and
worst ground penetration **0.442 mm**. Actual GLB and FBX reimports passed all
65 sampled times each, with maximum surface deviations 2.216 and 0.731 microns.
The preview renderer now preserves original materials on request and frames the
actual model automatically.

**Visual result: REWORK.** The three-cycle, 96-frame, 30-FPS full-mesh video was
sent to DEV as `ebed7e501952`. Upper forelimbs bend too low and form unnatural
bulky folds; the pose appears crouched. Contact and export metrics do not
override that failure. See `visual-review-v3.json`, `preview-v3`, and the
side-projection `rest-joint-audit.png`. A working hypothesis is that the external
forelimb guide attachment is being used as a shoulder even where it lies near
the elbow/belly line; this is not yet a confirmed fitting diagnosis. Inspect
shoulder/elbow placement and support stance, then validate any change with new
weights and actual mesh footage. Do not approve or publish these clips as a
finished animation library.

After that blocker is fixed: validate the corrected foreleg movement on the
full mesh; add run/sprint, jump phases, eating/rest and their entry/exit actions,
turns/braking, reactions/attacks/death/get-up; validate controller transitions,
different horse morphologies and additional animal families. The full goal
remains active.

### Forelimb placement and centered support stance

The first full-mesh rejection is now superseded by an offline anatomical
prototype. The primary semantic skeleton is pinned by SHA-256
`03b1bc4f8f1d13bb44fa38b74da47f75fc241dcb46e7eb00ce4be21ca8928837`.
Do not use the later `external_arp_fit_summary.full.json` for primary joint
coordinates: its timestamp belongs to a subsequent variant after the primary
Blend was saved. The original primary Blend remains unchanged.

The prototype locates the elbow from the stable lateral bank of the foreleg
guide and uses the canonical humerus/clavicle vectors to place shoulder and
scapula inside the torso. It preserves the fitted fetlock/toes and all hind
joint endpoints. Fresh Heat Map weights are transferred through the exact-weld
proxy to the original 119,996 render vertices. Saved-source validation confirms
the six proximal forelimb points are inside the mesh, the guide replay matches,
and positions, topology, normals, UVs and materials are preserved. This is an
offline prototype, not yet a converter runtime placement change.

`real-bay/shoulder-layout-probe-v1/anatomical_rig.blend` is pinned by
`703e3eabf54f376daf23408a0e342a00f6be41ac08aee5a751b0bbc395362691`.
The complete three-cycle full-mesh video `6d5fff7fc207` received a user
"Accept" verdict through the addressed DEV inbox. That accepts the shown
forelimb prototype; it does not approve unseen actions or the whole pipeline.

Authoring now supports an explicit per-limb `stance_center_joint`: `0` anchors
the support center beneath the proximal joint and `1` beneath the elbow.
The sole-to-fetlock offset is preserved; horizontal adjustment is bounded by
`max_stance_center_adjustment_height_fraction` (default 0.35, maximum 0.5).
Omitting the anchor preserves the prior source-foot center. Actual joint limits
and contact tolerances are unchanged. The horse experiment uses fore anchor 1
and hind anchor 0 to correct its stretched source stance. Walk stride 0.7 of
hip height becomes reachable at 0.519 model metres/second, versus 0.222 for the
previous short stride. These speeds refer to the normalized source scale.

The centered set contains five idles, forward/backward walk and trot. All eight
pass Blender key/half-frame ground-contact checks. Forward walk's maximum hoof
target error is 0.381 mm and ground penetration 0.365 mm. All eight Actions
survive export cleanup; removing the unused source world and orphan image data
reduces packed images from 101,022,098 to 3,946,811 bytes, and the complete Blend
is 12,186,562 bytes. Required mesh materials remain in the asset.

Evidence is in the project workspace `work/centered-stance-v1`: `all-actions`,
`clip-manifest.json`, `exports-all8`, and full-cycle walk/trot preview folders.
DEV videos `df1046762779` and `abac5605ef3e` show the longer walk in three-quarter
and side views. The renderer now accepts `--view three-quarter|side|front`.
The complete 32-frame walk and 24-frame trot sequences were visually inspected.
Twenty-nine authoring/timing tests pass. Further motion polish and transitions
remain required; generated clips retain `quality_approved=false`.

Game-weight fidelity is evaluated separately. Naively retaining the strongest
four of the prototype's nine influences produced 15.85 mm maximum error on the
short walk. Pose-aware reduction over that one clip reduced actual Blender
quarter-frame error to 1.552 mm, with GLB/FBX reimport errors 11.450/0.672
microns. That candidate is valid only for the exact short-walk clip hash and
must not be reused for this longer walk or other actions. The new DEV reducer
in the converter repository consumes safe numerical NPZ, an exact multi-clip
hash manifest and immutable input snapshots. It uses keys/halves for training
and disjoint quarter/three-quarter timestamps of the same declared clips for
holdout. The fixed 3 mm gate stays in force; this is not unseen-action testing.

The first eight-clip weight candidate correctly failed at 3.035 mm on six
vertices during `idle_look_around`. Training-only minimax refinement, including
alternate sparse supports where the initial selection still exceeds 3 mm,
reduced maximum holdout error to **2.677 mm**, with no sample above 3 mm.
It changes neither the clips nor the gate. The candidate NPZ SHA-256 is
`e01d1f7ef262e13758a2d3d56eb278fdc8b500dbc09b48628926ba988afdccf0`.
The reusable DEV tool is merged and pushed as converter main
`f3fb5d4368f1574f6b93d01cbd5aae34a5e2cc60`; nine independently executed Python
3.10 tests pass. This developer-only change requires no farm deployment.

Independent actual Blender comparison subsequently passed all **2,280**
quarter-frame samples across the eight clips. Maximum full-weight versus
four-weight surface deviation is **2.678 mm**, with no sample-vertex pair above
3 mm. The highest contact error is **0.727 mm** on trot, with ground penetration
0.725 mm; forward walk remains at 0.381 mm. Preconditions prove the active
armature modifier, exact vertex order/topology, clip hashes/times and nonzero
motion/contact coverage. Mesh/UV/normal/material/texture fingerprints remain
unchanged. `optimized-all8/weight-validation.json` records every action.

The new all-action Blend is 12,092,693 bytes, SHA-256
`5e1aabc8999bac962731632f311b0e204f46e05a28d92e7d31d8151cbfde2029`;
GLB is 12,139,508 bytes, SHA-256
`7e48ac282bcd4b15621c1a30efb6679de5e8230dd8169dbcc4d995e7641a6021`.
All eight FBX clips were also exported. Reimport verification now stores
numerical arrays and removes only exactly duplicate surface points. This
preserves bidirectional nearest-point distance while avoiding the prior
22.6-GB Python-tuple allocation on the full eight-action mesh. A Blender
comparison verifies exact equality of the distance with/without duplicates,
retains distinct points only 1e-10 apart, and rejects empty surfaces. The
original owned read-only verifier was stopped before restarting the bounded
version. All **16** actual reimports then passed: eight GLB actions and eight
FBX files, at every key and half-frame. Maximum surface deviation was
**13.374 microns for GLB** and **0.720 microns for FBX**. Numerical reference
storage was 549,174,912 bytes; the process was observed at 958,693,376 bytes
working set. The new side-view video is complete: 96 frames, 30 FPS, three
cycles. All 32 phases were inspected on the optimized full mesh. These checks
close the current eight-action skin/export candidate; they do not establish
controller transitions, the remaining action library or cross-model quality.

Two obsolete intermediate Blends were copied into the project workspace
`work/autorig-intermediate-archive-20260905`, hash-verified and removed from their
old R-drive paths when that drive filled. `manifest.json` records both original
paths and archive hashes. The primary source and active prototype are intact.

### Automatic Stage 3 integration

The accepted placement algorithm is now integrated into converter main
`493e6bf76b12b66fcbbae03e7ed24b1ea291b235`. Its narrow gate requires both preset
keys to identify HORSE_2 and both exact six-bone forelimb registry chains.
Canonical vectors are captured before semantic fitting; shoulder, elbow,
carpus and fetlock use the corresponding bone heads, and the canonical center
is the midpoint of both elbows. Both sides are computed before mutation.

Correction-time failure restores all 304 edit bones, including hierarchy,
connection flags and rolls, plus pending change rows and metrics. Every
non-target bone must remain unchanged during correction. After the normal
follower/autofix passes, any unexpected change to corrected joints or protected
feet/hind bones is a terminal error before publication. Geometry metrics are
recomputed from all seven joints of each six-bone chain; quality approval is
never inferred from the geometric gate.

The reusable actual-Blender fixture passed 14 success, rollback and drift
cases, including isolated interior-head, tail, roll, parent and connection
changes. An independent Python 3.10 regression run passed 59 relevant tests.
The fixture and evidence live in project workspace `work/forelimb-runtime-fixture`.
Local Blender has no licensed ARP installation, so these results deliberately
do not claim Match to Rig or final binding compatibility.

Normal artifact rollout installed the new commit on F11, F13 and F2. Artifact
SHA-256 is `8225253e5b430b423e93bf66cd1bfeee03d558e8800dd936a4eb8b859508db7c`
(444,188 bytes). Transfer/stage and apply durations were F11 3.273/12.743 s,
F13 2.794/13.481 s, F2 3.061/12.173 s. Exact boot commit/artifact, PID and cwd
matched on all three. F1's deployment guard aborted because the converter was
busy; it remains on `2c66008` with its active task intact. F7 was unavailable.
No tasks were interrupted and no rollback occurred. The complete report is in
`work/horse2-farm-rollout` in the project workspace.

An initial F11 submission was rejected before task creation because its central
identity was incomplete. Read-only log/ledger inspection confirmed that it
created no task. A fresh submission through the supported ordinary manual
queue was accepted as task `fc7ce0d6-9a71-481f-ac52-d8d4fed1bd81`, model
`3c12bf4f-05d0-4af1-874b-405fd3611548`. It uses the unchanged owned horse source
and reached **Completed** with no task error. Do not submit a duplicate.
Primary Stage 3 evidence was copied before later variants: summary SHA-256
`ca44fb48ce7f669b2f63fd4c24231c6158db118b0c17bc037e39a35bb0e2be0d`, timestamp
2026-09-05 16:43:00 UTC. Correction was applied and revalidated after followers;
Match to Rig and final-rig usability passed. All 119,996 vertices received
finite weights with zero uncovered/invalid vertices, and the exact-weld proxy
was removed. The complete primary Blender pipeline stage took 292.487 seconds
(including preprocessing; this is not the isolated Stage 3 fitting duration).

The saved primary Blend is 104,299,396 bytes, SHA-256
`3fb1d046fabac6e99d01e646199113b19bc5627a01dfd88a863b2e325acd299a`.
Direct inspection of its actual bound rig confirms 344 bones, 91 deform bones,
up to nine influences and maximum weight-sum residual 9.68e-7. Actual deform
shoulder/elbow/carpus/fetlock heads match the primary reference targets within
7.35e-8 metres. The exported GLB is 12,241,008 bytes, SHA-256
`92d00df47136d412ab5196f3861c2673c2cc3dd07b7041fd5181b1e5eaeea56c`; both files
were checked against remote hashes. Its native Horse_gallop spans 0.75 seconds.
The six-cycle side-view video (135 frames, 30 FPS) is published in DEV as
`a36e47458958`. This is a native preset deformation check, not approval of a
new authored gameplay gait. Preview framing now uses meshes bound to the
selected armature, excluding importer bone-widget geometry.

The task exposed a separate pre-existing optional-viewer failure: a
274-character Windows staging path prevented the first derivative from being
created after its geometry scan passed. The canonical GLB, Unity package and
preview completed. Converter main `869f7d5189ef1ece70a81462a9d986145138775e`
shortens only temporary basenames inside the existing unique stage directory.
Public filenames, geometry gates and paired installation/rollback remain
unchanged. Fifty relevant tests and actual Blender export of both derivatives
passed; temporary paths were 212 characters, with 114 weighted bones/ancestors,
restored bone flags and complete staging cleanup.

The subsequent deployment attempt could not reach any SSH endpoint and applied
no artifact: `ssh farm-f11` returned socket `Permission denied`. HTTP still
confirms F11 is healthy on `493e6bf`. Do not claim `869f7d5` deployed. Retry only
through the normal artifact controller after SSH connectivity is restored;
do not bypass it with direct source copies or network-setting changes. Reports
are in project workspace `work/viewer-short-path-rollout` and
`work/horse2-runtime-canary/viewer-diagnostic`. The actual viewer pair produced
locally is under `work/vq`; it has not been published to the worker.

### User-requested proportion review

The user rejected the native-gallop video as a flying horse and requested
comparison against multiple anatomical references and photographs. See
[the proportion review](horse-leg-proportions-review-2026-09-06.md) for the
corrected anatomical/control-point audit, original references, and the offline
carpus-ratio experiment. Target-coordinate agreement is not anatomical
acceptance. Do not expand the forelimb rollout while this review is in progress.

### Ten-action P3 tooling checkpoint

The authoring tool now supports explicit `run` and `sprint` profile recipes.
An absent non-idle recipe raises an error instead of silently authoring an
idle under a locomotion label. Existing profiles retain their previous
quintic trajectory. An optional `bounded_c2` swing decelerates stance velocity,
transfers the hoof, and restores touchdown velocity with continuous horizontal
position, velocity and acceleration. Its overshoot is explicitly bounded by
the chosen short easing interval. The same path drives the scapula.

P3's draft left-lead recipes use hind-right, hind-left, fore-right, fore-left
contact order, consistent with the [University of Kentucky gallop
description](https://horses.extension.org/horse-gallop/). Phase offsets, stance
duties and amplitudes are engineered pilot values, not measured mocap. Run has
21 keys over 20 intervals (0.667 s); sprint has 17 over 16 (0.533 s), both at
30 FPS. Normalized-model speeds are 1.669 and 2.704 m/s respectively. Do not
interpret these scale-dependent values as validated real-horse sprint speeds.

The exact ten-clip manifest under
`work/horse2-anatomy-audit/hind-experiment-v1/balanced-rig` was rebuilt and its
four-influence weights optimized. Actual Blender verification sampled 2,426
quarter frames; maximum skin deviation was 2.808 mm (run), below the unchanged
3 mm compression gate. Maximum contact target errors were 1.770 mm for run
and 4.741 mm for sprint; the latter still needs contact polish. All twenty
GLB/FBX reimports passed; maximum surface error was 12.19 microns for GLB and
0.732 microns for FBX.

The bridge now checks the final stance key even when the next key starts
swing. Interpolated contact checks still require both endpoint flags. An
independent audit checked every declared stance key for all ten clips. A
20 mm corruption of run's final left-fore stance key (frame 14) caused the
actual Blender bridge to fail before saving any Blend or GLB. Authoring and
timing regression suites pass 34 tests.

These results validate tooling and export fidelity only. Independent head
region inspection found wrong native weights and misplaced head/neck bones;
see the proportion review. The ten-action candidate is not ready for visual
acceptance or publication as a game-ready library. Correct the anatomical
baseline and regenerate weights and relevant evidence before continuing to
controller acceptance. No runtime animation library has been enabled.

### Repeatable Blender skin verification

`validate_quadruped_skin_blender.py` promotes the previously exercised local
verification procedure into a reusable single-mesh authoring tool. It takes
an authored Blend (beside its `export-report.json`), the exact reduction report,
and a fresh output directory. It derives the rig, full weights, optimized
weights and clips from hash-checked report inputs; there are no horse filename,
vertex-count or ten-action assumptions. Gameplay action identifiers must be
unique and safe for use as export filenames.

```powershell
blender.exe --background --factory-startup --python-exit-code 1 `
  --python tools/animation_fitting/validate_quadruped_skin_blender.py -- `
  --source work/candidate/exports/authored-candidates.blend `
  --reduction work/candidate/weights/animal-skin-weights-report.json `
  --output work/candidate/verified-exports
```

Use `--preflight-only` to check asset/manifest consistency without creating
the output directory. Full verification compares actual Blender surface
positions at every key and quarter frame against the full native weights,
checks every declared stance key including its last key, and requires the
unchanged 3 mm compression and 6 mm contact/ground gates. Mesh attributes and
packed material images must remain unchanged. Blend, GLB and per-action FBXs
are saved only after all checks pass. Format reimports remain a separate step.
This tool measures fidelity and contacts; independent anatomy and regional
binding checks must establish the correctness of its source baseline.

The promoted tool passed preflight on the preserved source fixture and actual
Blender rejection cases for unsafe and duplicate action identifiers before
creating an output directory. It then completed P4d's exact ten-action run:
2,426 quarter-frame samples, maximum deviation 2.249555 mm, unchanged geometry
and material fingerprints, and four influences. Independent ear-locality
validation and all twenty subsequent GLB/FBX reimports passed. The detailed
anatomical limits and delivered candidate identity are in the proportion
review; successful compression does not approve future actions automatically.
