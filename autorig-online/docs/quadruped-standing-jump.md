# First standing-jump candidate

The current landing iteration is documented in
[P8 prelanding refinement](quadruped-prelanding-refinement.md). This file
retains the P7 baseline and its evidence.

The current candidate is a vertical jump from idle, with a controller-owned
reference actor trajectory and separate local skeletal posture. It introduces
four v2 clips and a dedicated Blender consumer; legacy v1 readers still reject
v2 semantics. This is an offline motion prototype, not a completed engine
controller or an approved four-weight game library.

## Authored motion

`author_quadruped_jump.py` uses the P6 gameplay profile, unchanged native rest
geometry and the same bounded three-link leg IK. The experimental recipe is
`profiles/horse_jump.experimental.v1.json`. Its 30 Hz full sequence has 65
samples (64 intervals):

| Clip | Canonical samples | Keys | Playback |
| --- | --- | ---: | --- |
| jump_start | 0 through 24 | 25 | one-shot |
| jump_air | 24 through 32 | 9 | held local pose |
| jump_land | 32 through 64 | 33 | one-shot |
| jump_full | 0 through 64 | 65 | one-shot |

All shared poses match exactly, including quaternion orientation. The first
and final full-jump poses match the actual P6 `idle_neutral` first pose.
Forefeet lift at sample 12 and touch down at 40; hind feet lift at 16 and
touch down at 42. Spine and pelvis articulate through tuck/recovery, while
the middle air pose is deliberately held for variable runtime flight time.

The reference actor follows a vertical ballistic arc from sample 16 to 40.
For this normalized model, gravity is 3.090225 model m/s², takeoff speed is
1.23609 model m/s, flight time is 0.8 s and apex displacement is 0.247218 m.
These are engineering authoring parameters, not measured horse biomechanics.
Local crouch/landing offsets join the actor arc with continuous world-root
height and velocity. The ballistic displacement is not written into bones.

An initial shared tuck-pitch signal kept a hoof tilted after its target had
landed, causing about 1.125 mm penetration at keys. Foot pitch now follows
each foot's own lift envelope and is zero on contact and first-liftoff keys.
This reduced key penetration to approximately 4.1 µm without shifting targets
or relaxing joint bounds. QA is recomputed for each slice: the nine-key air
clip has nine airborne samples per foot, zero stance samples and null planted
measurements. Canonical full-jump physics is labelled separately.

## Blender and native evidence

`blender_quadruped_jump_bridge.py` validates v2 semantics, both source pins,
blueprint geometry and world-space evaluated surface. It stores the reference
actor path in the v2 sidecar and exports local skeletal poses. Source
coordinates and export-axis options are recorded explicitly; a future engine
consumer must transform the sidecar's authoring coordinates appropriately.

Normal surface failure creates a diagnostic JSON but no Blend/GLB/FBX.
`--diagnostic-only` can save a review Blend even after failure, without game
exports. Diagnostic status, surface QA and tolerances are embedded on the
armature as well as in the sidecar. Exported prototype weights still come
from the provisional authoring input; fresh native-weight reduction is a
separate required gate.

The source target-plane tolerance remains 0.515038 mm. Actual linear TRS
interpolation causes up to 2.301339 mm ground penetration at sample 40.5 and
2.578260 mm target error. This passes the established 6 mm evaluated-surface
gate but fails the stricter target band. Both results are retained. The
native diagnostic checks 257 key/quarter-frame poses, using the full CE9
weights. Direct scene-versus-numeric actor placement checks at start, apex
and finish agree within 0.0441 µm, confirming that display height is applied
once.

The landing remains abrupt: the largest adjacent local foreleg-bone change
is 27.304° at sample 40→41. A useful next refinement is a modest prelanding
bend before first contact, while retaining the same target planes, finite
joint bounds and continuous body trajectory. Evaluate it on the full mesh
before freezing a combined reduced-weight bank. Runtime contact IK and a
checked recovery blend also remain necessary for variable collision timing.

## Provenance and current artifacts

Project work is under `work/horse-jump-review`. V1 is retained as the
rejected shared-foot-pitch experiment. V2 fixes foot pitch; V3 fixes slice QA.
V3 also contained a misleading unused `source_sha256` pointing to the native
reference rather than the actual provisional authoring source. V4 corrects
it, and the authorer, bridge and renderer now reject that mismatch.

Selected JSON is in `v4`, with both source pins equal to
`4c9c15ab690267331287914829f745e5aa8a5598f6f1dd1c9a3ebb03b09b4075`.
Independent comparison confirms that every other V4 field, including poses,
contacts, targets and QA, is identical to V3. The compiled posture Blend is
also byte-identical, SHA-256
`3038be38d3326271a341043752eea2e5a19f87f2c8fe84fdf765203e0cb9d4b2`.

The retained native video was rendered from V3 native Blend
`746813c01920e378e9e6dda0fdae8cfc99f8cf387b740877a2cf124625825032`.
It is verified pose-equivalent evidence for corrected V4, not a claimed V4
rerender. Current renderer preflight accepts this native source with the
corrected V4 report and its matching skin proof, while rejecting the stale
V3 report. V4 native QA was run separately and matches the retained capture's
surface measurements. The video contains two complete attempts with neutral
pauses: 180 frames, 30 FPS, 960×540.

The focused verification total is 98 tests plus 22 subtests, including 13
jump-authoring tests and four actual Blender bridge cases. Those cases cover
external actor ownership, failure without exports, self-labelled diagnostic
output, and a lateral target error that passes the 6 mm surface gate while
correctly failing the stricter target band. The native V4 QA and stale-renderer
pin rejection are additional actual-asset checks. No production deployment,
combined-bank reduction or v2 GLB/FBX reimport approval is implied.
