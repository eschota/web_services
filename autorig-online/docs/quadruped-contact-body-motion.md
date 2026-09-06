# Contact-driven body authoring candidate

The P4d rig and native weights are retained. The previous run/sprint body
oscillation was independent of the support sequence. The new optional
`body_dynamics` recipe instead generates vertical body-root motion from the
declared stance intervals. Hoof targets, contact flags, cadence, segment
geometry and joint bounds remain unchanged; limb IK is recomputed to maintain
those targets while the torso moves.

Research describes gallop mechanics that do not reduce to a simple vertical
spring-mass model, and distinguishes trunk motion from measured centre-of-mass
motion. See [Pfau, Witte and Wilson, 2006](https://doi.org/10.1242/jeb.02439).
Measured galloping forces also vary with speed and show different propulsive
and braking roles for hind and fore limbs; a fixed standing load ratio is not
a universal gallop rule. See [Ground reaction forces of overground galloping, 2019](https://pubmed.ncbi.nlm.nih.gov/31444280/).
These sources motivate phase-aware authoring; the present pulses, scale and
pitch response are engineering choices, not fits to those measurements.

## Model and boundary conditions

`contact_body_motion.py` assigns each limb a nonnegative raised-cosine vertical
force pulse only during stance. Its integral is the limb's explicit share of
the stride's vertical impulse. Shares sum to one, so total vertical impulse
balances gravity over the cycle. Analytic integration and one initial-velocity
constant close both height and velocity. Acceleration is continuous, and in
the all-air interval it equals the negative configured model gravity.

Pitch is a separate style response to fore-minus-hind loading. It is **not**
a solved torque or rigid-body simulation. Rotation uses the mean limb-root
point as its pivot, with translation compensation; it does not orbit the
torso around the ground-level export origin. This pivot is not a measured
centre of mass. The current implementation requires an unparented export root
at the origin. Legacy profiles without `body_dynamics` retain their old motion.

The normalized gravity, impulse shares and pitch gain live in
`profiles/horse_p4d_contact_body.experimental.v1.json`. No production/default
profile was activated. The current pilot uses gravity 7.5 limb-root heights
per second squared, equal impulse shares and pitch gain 0.02 radians per unit
load difference. On P4d this is model gravity 3.863 length units/s². It must not
be presented as a calibrated real-horse mass/scale model.

## Executed pilot

Evidence is under project `work/horse-gait-force-review/v1`.
The source and candidate use the same full native P4d weights for visual
comparison. The new `run` and `sprint` clips are still 21/17 samples at 30 FPS.
Their body-height calibration remains inside the existing cap: additional
drop 0.010/0.005 root heights. No joint limit or hoof target was relaxed.

Run's actual authored torso-pivot vertical range changes from 6.707 to
65.004 mm and pitch range from 0.917° to 4.146°. Sprint's vertical range is
42.485 mm and pitch range 5.620°. These are candidate measurements in the
normalized model's scale, not anatomical acceptance thresholds. The 30 FPS
animation samples the analytic target; it is not a runtime physics solver.

The ten-action bank keeps eight P4d clip files byte-identical and replaces
only run/sprint. Fresh four-weight reduction passes the 3 mm holdout gate
(maximum 2.139139 mm). Actual Blender evaluation over 2,426 quarter frames
passes with maximum skin deviation 2.154081 mm. Maximum contact error is
3.572140 mm; source geometry and rest skeleton remain unchanged. Independent
ear-locality tests remain clean. All twenty actual GLB/FBX reimports passed:
maximum surface differences are 9.605 microns for GLB and 1.203 microns for
FBX. Visual gait acceptance remains separate. The old P4d optimized weights
are not reused for this changed clip manifest.

The delivered P5 GLB is 12,269,220 bytes, SHA-256
`18283370c77af6da7f5aefb047d0a066f8e6ded27e9be8f9d499ecec565a37b7`.
The optimized Blend SHA-256 is
`6edff056c01cb5d2dd67ba888da1e6716febc58e5e59e95b531439caef96d033`.
The local review copy is `outputs/horse-gameplay-p5.glb`. The full-native
before/after run comparison is `outputs/horse-gallop-body-comparison.mp4`
(120 frames, six cycles, 30 FPS, 1920x540), published in DEV `169d4f2d7f15`.
The accompanying plot is `outputs/horse-gallop-support-phases.png`.

Forty-three authoring/timing tests pass, including impulse balance, wrapped
stance intervals, ballistic flight, periodic position/velocity/acceleration,
finite-difference checks, torso-pivot preservation, planted feet with and
without actor translation, invalid motion-root rejection and legacy behavior.

Remaining work includes visual gait refinement and the missing jump,
landing, turning, reaction and controller-transition actions. Neither this
model nor its numerical PASS makes the complete gameplay pipeline ready.
