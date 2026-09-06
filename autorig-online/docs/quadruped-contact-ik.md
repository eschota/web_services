# Nominal contact-IK replay, P9

`quadruped_contact_ik.py` adds a bounded correction kernel for an already
sampled actor-local pose. Actor translation is subtracted from world targets
once. The existing three-link sagittal IK solves active flat-foot contacts,
using the current joint angles as its posture prior. Root, torso and inactive
leg local matrices are retained. Inputs and rig data are not mutated, and a
failure in any active leg prevents a partial result from being published.

The maximum correction is 8° on every changed local bone, including primary
joints, intermediate bones, foot and toes. Checking only the three solver
angles was insufficient: an early adversarial test could reset a tilted foot
by almost 20°. The final kernel measures sign-invariant rotation deltas for
all changed bones. Active output rotations are projected to SO(3); unit-scale
rigid affine inputs are required. A tight endpoint-error cap rejects targets
outside the supported sagittal plane instead of approximating them loosely.

## Sampling and contact windows

`sample_local_pose` reproduces linear local translation and normalized linear
quaternion interpolation with hemisphere continuity. This matches the v2
Blender bridge's linear quaternion channels. The probe uses the runtime event
state at floor(frame), lasting until the next contact event. For example, the
last hind stance interval [15,16) remains active until liftoff at 16; its target
is still on the plane. The older conservative both-endpoint QA mask is retained
separately in the envelope. Active interpolated targets must remain on the
declared plane; they are not moved to hide a failure.

`probe_quadruped_contact_ik.py` writes an explicit 120 Hz diagnostic pose
envelope with original/corrected matrices and source pins. It is not a new
animation clip and must not be fed to a v2 clip consumer as if it were one.
The immutable P8 canonical clips remain at 30 Hz.

## Actual native result

Work is in `work/horse-contact-ik/v2`. The source is P8's retained native Blend
`cd043304735555f221c005c5630193e75359595b2c166dfefeb436b3f68c3bf0`,
with the CE9 full native weights. The envelope is pinned to the corrected
`landing-preload/p04-final` report and its snapshotted recipes.

Across 257 quarter-frame poses, actual Blender measurements are:

| Measure | Original P8 | Contact IK |
| --- | ---: | ---: |
| Maximum active-contact target error | 1.325917 mm | 0.000205 mm |
| Maximum ground penetration | 1.029422 mm | 0.000183 mm |
| Maximum local bone correction | — | 0.762440° |

These are numerical measurements in normalized model units, not physical
sensor accuracy. Inactive/swing-foot target error still reaches 0.757151 mm;
it is not presented as a full target-band pass. Active contact and clearance
pass the 0.515038 mm target band. The replay is nominal, translation-only and
on a horizontal reference plane. Early/late/absent collision, runtime state
blending, terrain adaptation and engine integration are not yet implemented
by this result.

The native validator reconstructs the exact reference actor/targets/contact
state, checks the full sample grid, matrix coverage, rigid transforms,
unchanged non-contact bones and the 8° cap. It then evaluates the actual mesh,
applied matrices and joint bounds. Source unkeyed pose scales are restored
before every baseline sample: an initial probe accidentally retained tiny
scale residues from manual correction and failed parity. Corrected poses use
the contract's unit scales. Final sampler/Bpy matrix error is 5.48e-7, applied
matrix error 2.86e-6 and maximum joint-bound violation 2.05e-7 radians, within
explicit numerical tolerances. Source files remain unchanged.

## Visible evidence and remaining work

`render_quadruped_contact_ik.py` shows a forefoot close-up around source frames
38–44, before/after on the same native mesh and camera. It displays quarter
poses at 30 FPS, giving 4× slow motion. There are 25 distinct poses per side and
135 displayed frames with explicit holds/repeats. This is contact evidence,
not a demonstration of a completed game controller.

Next, integrate the kernel with the collision-gated `LandingPlayback` pose
compositor and check early, late and absent collisions without claiming foot
support before it is achieved. Preserve bounded corrections and separate
actor motion. Include verified transition/IK pose envelopes in subsequent
native-to-four-weight reduction validation; old exact-clip reductions do not
automatically cover procedural corrections. Mixed-bank support, v2 format
reimports and the remaining gameplay actions still remain.
