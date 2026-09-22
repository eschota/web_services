# «План на двоих» — four-shot Wan-Animate-2 review

Date: 2026-09-22

## Verdict

All four generated shots are accepted for this bounded story sequence, with
the limitations listed below. This is evidence for these exact keyframes,
driving clips, prompts, seeds and four-second outputs. It is not a universal
Wan-Animate-2 quality claim, and it does not prove arbitrary new characters,
camera moves, props, multi-person layouts or lip-sync.

Each true ComfyUI output was reviewed across all 97 frames in four consecutive
ranges (`0–24`, `25–49`, `50–74`, `75–96`) against the corresponding motion
reference and approved story keyframe. Root additionally verified technical
browser playback. All public artifacts were downloaded again after the
transport repair and match the reviewed ComfyUI output SHA-256 exactly.

Production graph: https://autorig.online/nodes?g=a7efd447a3f6<br>
Source graph: `30cb8f68083f`<br>
Story: **План на двоих** (working title: “The Lost Note”)<br>
Final video: https://autorig.online/renderfin/render/default_user/maya-leo-plan-for-two-20260922-7869863d.mp4<br>
Common delivery contract: H.264 yuv420p, 960×540, 24 FPS, 97 frames,
4.041667 seconds.

## Transport incident and repair

The first public receipt initially pointed at the uploaded driving input
instead of the generated SaveVideo output. That wrong file was 480×270 and
showed the source actress in a white hoodie. It was not model output and is not
used for any identity or quality verdict.

The cause was deterministic: native Comfy history contained both `type=input`
and `type=output` video artifacts, while the old resolver selected the first
video. Release `R` was built from an older local adapter and therefore also
lacked the production `managed_identity` submission argument. Release `R1`
restored the exact production adapter and added output-only selection. No GPU
task was resubmitted. Shot 1 and shot 2 public receipts were repaired from
their existing true outputs, and all four public files now match the reviewed
hashes below.

## Shot reviews

### Shot 1 — Maya reads the message

Task: `9ab7a533-51f1-4491-918b-6f0c5df1b697`<br>
Public MP4: https://autorig.online/renderfin/render/default_user/9ab7a533-51f1-4491-918b-6f0c5df1b697.mp4<br>
SHA-256: `474a981865a909fb5c69af38b6a565f0dee2c26a54de490ec6fca0a7801e348a`

- Maya matches the approved `shot-1-v2.png`: straight dark bob with uncovered
  crown, mustard jacket and cream top. The source actor's white hood does not
  leak into the true output.
- One black phone and one attached phone hand remain stable. No extra person,
  hand or phone appears.
- Her face remains recognizable through subtle gaze and head movement. The
  green outdoor background and framing remain fixed.
- Action is deliberately restrained: read phone, shift gaze, remain focused.

Disposition: **accepted for this story**. Limitation: no speech/lip-sync is
tested, and fine fingertip-phone contact remains partly resolution-limited.

### Shot 2 — Leo walks to the meeting

Task: `2d96f091-ae43-41a3-a95d-a4a0a0e620a5`<br>
Public MP4: https://autorig.online/renderfin/render/default_user/2d96f091-ae43-41a3-a95d-a4a0a0e620a5.mp4<br>
SHA-256: `4cda16e205237f0bedda0bbe850528e74595077d4f37c83ccc041b27c98bb12b`

- Leo's face, swept dark hair, beard and teal shirt remain stable in all 97
  frames. One person remains in side profile; no source-actor identity returns.
- The gentle side tracking and upper-body walk rhythm are preserved.
- The crop excludes legs and nearly all hands, so this shot cannot qualify
  gait, feet or hand anatomy beyond the visible torso/shoulder motion.

Disposition: **accepted for this story**. Limitation: walking is communicated
through tracking and upper-body motion; hands, feet and full gait are occluded.

### Shot 3 — Maya and Leo decode the plan

Task: `19215ab5-5964-4753-84b9-c089a312e6ca`<br>
Public MP4: https://autorig.online/renderfin/render/default_user/19215ab5-5964-4753-84b9-c089a312e6ca.mp4<br>
SHA-256: `b166f3cd722d8b3ff1260282815bf64f08abd2d2ab944c139efb785b91ae77e9`

- Maya stays left in mustard and Leo stays right in teal. Their identities do
  not swap, blend or collapse.
- Exactly two principal people remain. The paper plan and laptop persist while
  both lean and trace/point toward the plan.
- Visible hands remain attached and assigned to the correct actor. Pen/finger
  detail is small, but no definite extra or fused hand appears in the exhaustive
  frame sheets.
- Faces, wardrobe, screen positions, office geometry and table layout remain
  coherent.

Disposition: **accepted for this story**. Limitation: small hand/pen contact is
resolution-limited and there is no dialogue/lip-sync test.

### Shot 4 — cafe resolution

Task: `27804ff5-cf84-409f-bbe8-bbe5b51d1890`<br>
Public MP4: https://autorig.online/renderfin/render/default_user/27804ff5-cf84-409f-bbe8-bbe5b51d1890.mp4<br>
SHA-256: `cf58c2c3d25c5e955725ba989622cb7aeac955e9f56d13fd9fb1d921388a8bcd`

- Leo remains facing camera in teal with stable face, hair and beard. Maya
  remains in the intended over-the-shoulder position with dark bob and mustard
  jacket.
- Exactly two principals remain. Leo's clasped hands stay coherent and attached
  across the quiet smile-to-conversation motion.
- Cafe framing, background people, table and warm lights remain stable.
- Maya's face is intentionally hidden by the source composition; this shot
  cannot independently prove her facial identity.

Disposition: **accepted for this story**. Limitations: Maya facial identity is
not visible, hand motion is minimal, and no lip-sync is tested.

## Cross-shot continuity

- Maya keeps the same straight dark bob, mustard jacket and cream top in shots
  1, 3 and the back-view shot 4.
- Leo keeps the same dark swept hair, beard, teal overshirt and dark undershirt
  in shots 2, 3 and 4.
- Actor assignment remains consistent: Maya owns the phone/message beat; Leo
  owns the approach; both share the plan beat; Maya remains foreground and Leo
  faces camera in the cafe resolution.
- The four clips form a readable sequence: message → approach → joint decoding
  → relieved cafe conversation.

The assembled **План на двоих** video is 960×540, 24 FPS, 388 frames,
16.166667 seconds, SHA-256
`7869863d0b85a494d02c9d561f08e5a4ae74402aa029a929bdbd1e1f5c4bc990`.
Production browser verification measured `readyState=4`, `loop=true`,
`paused=false`, playback rate 1.0 and opened the lightbox. The watermarked
public motion references are evaluation sources; production use still requires
appropriately licensed source media.
