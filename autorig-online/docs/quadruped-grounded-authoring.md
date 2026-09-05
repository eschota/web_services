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
