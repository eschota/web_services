# LTX 2.3 first-frame baseline: exhaustive-frame review, cases 4–6

Date: 2026-09-22

## Review method and limit

This review covers the three completed tasks below. For every case, all 97
generated frames were decoded and inspected in four consecutive sheets
(`0–24`, `25–49`, `50–74`, `75–96`). The four-second 25 FPS source was
resampled to a 97-position, 24 FPS inspection timeline; the last source frame
was repeated for the final generated timestamp. Source and generated timelines
were then checked in the same four ranges. Key frames were also inspected at
larger size.

This is stronger than a sparse five-frame contact sheet, but it is still a
frame-sequence review rather than normal-speed continuous playback with audio.
It can expose one-frame anatomy/identity defects and action order, but it does
not fully judge motion cadence or perceived flicker at playback speed. A
`candidate` disposition below is therefore not final production acceptance.

All review derivatives are preserved under
`.codex_tmp/avatar-video-20260922/own-review-next3/`. Original sources,
generated MP4s, benchmark state and delivery artifacts were not modified.

## Shared generation contract

All three outputs use LTX 2.3 first-frame conditioning, 960×540, H.264
yuv420p, 24 FPS, 97 decoded frames, duration 4.041667 seconds, seed `9221001`,
8 steps, CFG 1.0 and sampler `euler_ancestral_cfg_pp`. No driving-video
control was supplied.

Scores use 0–5 and reflect this exhaustive-frame inspection. Hard rejection
conditions include material face/identity warp, body/limb failure, principal
people-count change, object/body fusion, or action reversal/absence.

## `face-object-interaction`

Task: `2c5663a1-61c7-4d32-b5ef-59f4ca71f2e5`<br>
Output: https://autorig.online/renderfin/render/default_user/2c5663a1-61c7-4d32-b5ef-59f4ca71f2e5.mp4<br>
SHA-256: `77abdeb0b12dc65a3202870bbb413a3678a2832293dc2e75eb285cbe4dd4c085`

Visible evidence across all 97 output frames:

- One woman remains centered with stable hair, shirt, background and broad
  facial identity. No face swap, duplicate face or body warp was found.
- The single silver can remains at her mouth throughout, matching the source
  action. Its circular base, cylindrical body and reflective appearance remain
  recognizable.
- The holding hand remains attached. Individual fingers are partly hidden by
  the can and watermark; no definite extra, missing or fused digit was visible
  at the available resolution.
- Head tilt and can position vary smoothly across the ordered frames. The
  camera and dark green background remain fixed.
- The source watermark/QR is preserved; that is input fidelity, but this stock
  preview must not be treated as a clean production plate.

| Criterion | Score / 5 |
|---|---:|
| Face quality | 4 |
| Hand/object anatomy | 4 |
| Identity continuity | 4 |
| Action fidelity | 5 |
| Principal people/object count | 5 |
| Camera/composition | 5 |
| Frame-to-frame continuity | 4 |

Disposition: **candidate** for this simple sustained object interaction.
Normal-speed playback and a clean licensed source remain required before
acceptance.

## `two-person-cafe-conversation`

Task: `38dcf126-20bd-4e93-a4ec-28bc0db33dd3`<br>
Output: https://autorig.online/renderfin/render/default_user/38dcf126-20bd-4e93-a4ec-28bc0db33dd3.mp4<br>
SHA-256: `b1eeec1a399c73fc5e0b93150b6cf98a784a8276a86467796ba71447b0739944`

Visible evidence across all 97 output frames:

- The two principal adults, over-the-shoulder composition, table, cups and cafe
  background remain present. No principal-person duplication or identity swap
  was found.
- The man's face remains recognizable, but mouth and lower-face geometry drift
  moderately during stronger generated speech expressions. This was not a
  total face collapse, yet it lowers face/identity confidence.
- The generated action differs materially from the source. In the source the
  foreground woman supplies most of the visible hand gesture while the man is
  comparatively restrained. In the output the man becomes the dominant
  speaker and performs a new large two-hand gesture late in the clip.
- The man's late hand shapes remain attached, but some finger configurations
  are soft/ambiguous at full-frame resolution. No definite extra arm was found.
- Camera and background are stable. The clip conveys “conversation,” but not
  the same participant-specific performance.

| Criterion | Score / 5 |
|---|---:|
| Face quality | 3 |
| Hand/body anatomy | 3 |
| Identity continuity | 3 |
| Action fidelity | 2 |
| Principal people/object count | 5 |
| Camera/composition | 5 |
| Frame-to-frame continuity | 4 |

Disposition: **rejected for source-action reenactment**. It may be usable as
loosely prompted conversation B-roll, but it fails the requested participant-
specific gesture transfer. Pose/driving-video control should be evaluated on
this case; prompt changes alone cannot specify the full temporal ownership of
the action.

## `seated-couple-object-action`

Task: `51bc45df-4b93-48a5-834c-ed869dba976b`<br>
Output: https://autorig.online/renderfin/render/default_user/51bc45df-4b93-48a5-834c-ed869dba976b.mp4<br>
SHA-256: `55278591b1195be9bd8e49d500fe0a726df3e5fad9565b8114e12c2efc8c60d1`

Visible evidence across all 97 output frames:

- Both adults preserve recognizable faces, clothing and body separation. The
  opening kiss separates naturally without a visible merged-face residue or
  identity swap.
- The ordered source action is reproduced: kiss, separation, attention moves
  to the table, then the man cuts/serves the cake while the woman watches.
- The knife/utensil, cake, plates, mugs, vase and table persist. The man's hand
  stays attached through the cutting motion. Small finger/utensil contact is
  partly obscured by resolution and watermark, but no definite hand-object
  fusion or extra limb was found in the 97 frames.
- The woman remains seated and separate from the man's cutting arm. Both faces
  remain stable through the later object interaction.
- Camera, kitchen geometry and table layout remain stable.

| Criterion | Score / 5 |
|---|---:|
| Face quality | 4 |
| Hand/object anatomy | 4 |
| Identity continuity | 4 |
| Action fidelity | 5 |
| Principal people/object count | 5 |
| Camera/composition | 5 |
| Frame-to-frame continuity | 4 |

Disposition: **strongest candidate in the second batch**. It still requires
normal-speed playback and later Avatar-identity replacement evidence before it
can qualify a reusable-character pipeline.

## Batch decision

The first-frame baseline now has six completed cases. For the new three:

- keep `face-object-interaction` as a candidate for a sustained, nearly static
  face/object action;
- reject `two-person-cafe-conversation` for exact action reenactment because
  gesture ownership and performance diverge;
- keep `seated-couple-object-action` as the strongest candidate because it
  preserves people count, identities, action order and the cake interaction in
  every inspected output frame.

These outcomes reinforce a scene-dependent routing rule. First-frame + prompt
can work when the desired action is visually anchored and simple, and it can
occasionally reconstruct a short ordered interaction. It is not reliable for
transferring who performs which gesture in a multi-person scene. The cafe case
should be prioritized for Pose/driving-video control and identity-separation
testing. No result in this document proves the persistent Avatar goal or the
final two-character story.
