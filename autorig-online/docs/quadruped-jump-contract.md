# Jump authoring and controller contract

Status: implementation contract for the next offline milestone. No jump has
yet been authored, exported or activated. P6v2 and its completed checks are
unchanged. The call-site audit is retained in the project at
`work/horse-jump-contract-audit.md`.

## One owner for world movement

Use an actor transform above the visual armature. The controller owns actor
translation and gravity. The animation owns local posture: crouch, pelvis,
spine, neck and limb motion. A local body-height offset for crouching is a
pose offset; it must not contain the ballistic actor trajectory.

The authoring tool records a separate reference actor trajectory for preview
and QA. Apply it once to the actor transform when evaluating world-space
vertices and hoof targets. Never add it to both the actor and skeleton root.
At runtime the controller continues its current position and velocity across
animation transitions; it does not reset to a clip's reference trajectory.
This also permits different jump distances without regenerating the whole
skeleton animation.

Initially support controller-owned world translation only. A future baked
root-motion variant needs a separate explicit contract and validation; do not
infer it from a nonzero root track or reuse the current boolean flag.

## Versioned clip semantics

Keep existing `autorig-authored-quadruped-clip.v1` validation unchanged. Add a
v2 reader/writer together, with these required concepts:

| Field | Meaning |
| --- | --- |
| `playback.mode` | `loop`, `one_shot` or `hold`; explicit seam policy |
| `motion.world_owner` | `controller` for this milestone |
| `motion.pose_root` | Exact unparented export-root bone |
| `motion.pose_space` | Actor-local skeleton coordinates |
| `motion.baked_actor_translation` | Must be false |
| `reference_actor_motion` | Finite one-shot sampled actor transform for preview/QA |
| `ground` | `space=reference_world`, plane height and tolerance in that space |
| `phases` | Ordered half-open support/flight sample ranges |
| `contacts` | Four complete boolean foot tracks, as in v1 |
| `events` | Per-foot liftoff/touchdown derived from contact-track changes |

All canonical QA streams share the 30 Hz timeline and exact sample count.
For N samples, phases use half-open sample-index ranges `[start, end)` and
partition `[0, N)` without gaps or overlap; the final sample belongs to the
last range. There are N-1 elapsed-time intervals. Between keys a foot is
tested as planted only when both endpoint contacts are true; other intervals
still undergo target, clearance and deformation checks. Preserve all
four foot identities and their verified sole anchors even in flight. Missing
tracks are errors. A flight interval requires all four contacts to be false;
a support interval requires at least one declared supporting foot. An action
may keep a particular foot's contact false throughout only when its explicit
phases permit that state. This replaces neither joint bounds nor geometric
checks.

Apply the reference actor transform to the actor-local evaluated surface and
targets first, then measure planted-foot height relative to the declared
reference-world ground plane. Plane height, tolerance and all measured
distances use the same normalized model metres. Measure planted-foot sliding
in that same space; do not compare a world-space foot with an actor-local
plane. Airborne samples still require finite targets, bounded
joints, mesh-ground clearance and accurate skin deformation. A zero-contact
interval is not a reason to omit all geometry checks or report an empty
measurement as a successful contact measurement.

Define events at sampled contact-state changes: liftoff is the first sample
with false contact after true; touchdown is the first true after false.
Record the incoming contact state to make an event at frame zero unambiguous.
Actual collision determines runtime grounding; reference animation events
describe the authored motion and must not force a timer-based collision.

## Air playback and transition poses

The existing `jump_air` static-loop timing is not inherently a bug. For a
controller with variable flight duration it can represent an actor-local
airborne pose that loops or holds. It must not loop a ballistic world path.
A finite cinematic airborne segment would instead declare one-shot playback.
Do not silently change the existing timing taxonomy to select that different
meaning.

The reference actor-motion stream is always a finite canonical one-shot QA
stream, regardless of pose playback mode. It is never repeated or extended
by `loop` or `hold`. At runtime only the actor-local pose and contact state
are extended while the controller integrates the actual trajectory. A held
air pose preserves its final flight state with all four contacts false.
An airborne loop requires every interval, including its wrapped seam, to
remain airborne with all four contacts false.

`jump_start` begins in a verified support pose and ends in an airborne pose.
Its exit and `jump_air` entry must match in local posture. `jump_land` needs
an explicit airborne approach followed by touchdown and recovery, rather
than an instantaneous cut from folded legs to a planted stance. Trigger that
approach using controller descent/ground proximity; maintain actor physics
until actual contact. Add an explicit synchronization point before authored
touchdown: approach playback may adjust speed and then hold a precontact
pose, but cannot enter planted touchdown/recovery without actual collision.
If collision arrives early, start a bounded contact-constrained recovery
blend from the current evaluated posture, rather than skipping instantly to
the canonical touchdown pose. If it arrives late, keep the precontact state
airborne; a timer must not start recovery in mid-air. Ground-proximity loss
can return to the air state through a checked blend. Test early, late and
absent collisions separately from the ideal reference sequence.
The air-to-landing join is checked on the actual mesh,
including the extension needed before the first contact. Return the final
landing posture to the declared idle or locomotion entry pose.

Check local translation and sign-invariant quaternion differences at joins,
then check evaluated skin and foot velocities through the blended transition.
Identical pose labels or contact flags alone do not prove a smooth join.
One-shot actions preserve their final pose and do not require first/last pose
equality. Loop and hold behavior must be explicit in the runtime sidecar;
GLB/FBX surface fidelity alone does not establish engine playback settings.

## Full jump preview and timing

Build `jump_full` from a continuous reference actor trajectory and the same
start/air/landing posture functions. The current 49-sample budget is an
authoring default, not a mandate to speed up the composed motion. At 30 Hz,
assembling three clips with shared endpoints uses the sum of their interval
counts plus one sample. Select an explicit `timing(..., samples=...)` override
when necessary. Never independently rescale the skeleton and ballistic arc.
Reference gravity, flight time, apex and takeoff velocity must agree; model
normalization must be recorded alongside these engineering parameters.

The preview must play the complete one-shot and its transitions. The current
renderer's modulo/repetition path is for looped gait evidence and cannot be
used unchanged for a jump montage. A looping air pose is a separate test from
one continuous takeoff-flight-landing sequence.

## Implementation order and acceptance

1. Add the v2 semantics validator and isolated tests for explicit airborne
   intervals, event boundaries, ground coordinates, one-shot endpoints and
   single application of actor translation. Reject malformed v2 data while
   preserving all v1 behavior.
2. Add posture/trajectory authoring with the existing bounded IK and native
   P4d reference. Separate actor movement from pose-root offsets before any
   keyframes are written. Assert the full exact source/profile pins.
3. Teach the Blender bridge and skin validator to consume the same validated
   context. Update preview playback and export sidecar semantics. Verify the
   actual controller package preserves loop/hold/one-shot behavior.
4. Generate and inspect the full native mesh over takeoff, flight, landing and
   transitions. Preserve the P6 spine and shoulder checks; new movement can
   expose new deformation problems even when gait checks passed.
5. Create a fresh combined clip manifest, reduce native weights again, then
   perform actual Blender and GLB/FBX reimport checks. The reducer is already
   frame-generic, but its old candidate is valid only for its old clip hashes.

Do not emit a v2 candidate through a consumer that silently ignores its new
motion/contact fields. Until all consumers understand the contract, the
existing authoring CLI continues to reject unsupported jump actions.
