# Prelanding refinement, P8

The next implemented layer is [nominal contact IK](quadruped-contact-ik.md).

An explicit optional `landing_preload_height_fraction` bends the legs before
first touchdown. Default zero retains P7 behavior. The selected diagnostic
profile, `horse_jump_prelanding.experimental.v1.json`, uses 0.04 times the
normalized limb-root height; the original profile remains at zero.

The root offset eases into preload during samples 32–40, keeps that offset
during the existing impact absorption at 40–44, then recovers by sample 64.
Total crouch plus preload is capped at 0.14 of limb-root height. The actor arc,
hoof targets, contact flags, foot pitch, timing and joint bounds are unchanged.
The air cut at 32, idle endpoints and C1 world-root landing join remain valid.

| Measurement | P7 | P8, preload 0.04 |
| --- | ---: | ---: |
| Maximum adjacent local foreleg-bone rotation | 27.304° | 19.625° |
| Native maximum target error | 2.578 mm | 1.326 mm |
| Native maximum ground penetration | 2.301 mm | 1.029 mm |

Native surface QA covers 257 key/quarter-frame poses. The 6 mm surface gate
passes, but the stricter 0.515 mm target band still fails. Preload 0.02 was
also evaluated and gave a larger 22.377° maximum rotation step. P8 improves
the reference, but landing still looks abrupt and remains a diagnostic
candidate. Further landing/contact refinement and runtime foot IK precede
freezing a game-approved combined weight bank.

## Verification and provenance

Work is under `work/horse-jump-review/landing-preload`. Raw `p02`/`p04`
experiments are preserved. Their prototype API calls used placeholder profile
hash fields, so they are not the selected provenance record. `p04-final` was
regenerated through the CLI using the saved profile and actual file hashes.
Motion and QA are identical; only recipe provenance differs. The authorer now
rejects malformed SHA-256 arguments and computes canonical profile-content
hashes internally. It snapshots the two recipe files inside each new output
bank. Dedicated consumers require all four recipe pins, verify file bytes
and canonical content against those snapshots, and reject missing, placeholder
or changed recipes. The intermediate `p04-pinned` is retained as history.

Both raw and corrected compiled posture Blends have SHA-256
`4ec12cc43f6fce90cd8262d9d18fc0b30194cc9feb7afba6c208ac770020650b`.
The retained native Blend is
`cd043304735555f221c005c5630193e75359595b2c166dfefeb436b3f68c3bf0`,
with the unchanged CE9 full native weights. Renderer preflight verifies this
source against the corrected `p04-final` report and matching skin proof.

The comparison uses P7's reconstructed world bounds for the same camera
framing. The deterministic renderer now renders 65 distinct sample poses
once and copies exact images for holds and the repeated reference sequence.
The result remains 180 frames at 30 FPS. Capture metadata records framing
path/hash/bounds, camera and cached-pose hashes. The already completed P8
capture has a clearly labelled post-capture provenance supplement; all 180
frame copies were checked against their 65 source pose images.

`quadruped_surface_blender.actor_local_points` uses Blender `foreach_get` and
array transforms instead of constructing a Python vector per vertex.
Blender canaries compare the result with the earlier reader. The unchanged
P7 native baseline retains its prior metrics; its complete QA now takes about
six seconds on this host. This optimization does not change weights, poses
or the reference actor transform.

Deliverables are `outputs/horse-standing-jump-p8-native.mp4`,
`horse-standing-jump-p8-validation.json`, and the side-by-side
`horse-standing-jump-p7-p8-comparison.mp4`. P8 native video SHA-256 is
`458bd3a98c63b41af7189ca4c51ba119540d113d3e5b618d1b1f35193c2214ab`.
No combined reduction, v2 format-reimport approval or production controller
deployment is claimed by this checkpoint.
