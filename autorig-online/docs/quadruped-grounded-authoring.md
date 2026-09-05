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

After that blocker is fixed: validate the corrected foreleg movement on the
full mesh; add run/sprint, jump phases, eating/rest and their entry/exit actions,
turns/braking, reactions/attacks/death/get-up; validate controller transitions,
different horse morphologies and additional animal families. The full goal
remains active.
