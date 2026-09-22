# LTX 2.3 first-frame baseline: visual review of the first three human clips

Date: 2026-09-22

## Scope and evidence

This is a bounded manual review of the first three completed jobs in
`adult-human-reenactment-ltx23-baseline-20260922`. A Renderfin `completed`
status proves only that a playable artifact was produced. It does not prove
face quality, anatomy, action fidelity, identity continuity, or suitability as
a video-reenactment pipeline.

The review used:

- 24 evenly sampled source frames and 24 evenly sampled generated frames per
  four-second segment;
- 12 source/generated paired timeline samples per case;
- ffprobe of the source segment, generated video, and comparison video.

The full videos were not watched continuously in this review. Temporal scores
below are therefore sample-based and provisional. They do not prove that no
one-frame defect exists between samples. All source, generated, and comparison
files were preserved unchanged. Extra review sheets live under
`.codex_tmp/avatar-video-20260922/own-review/`.

## Common pipeline

| Setting | Value |
|---|---|
| Pipeline | `ltx23-first-frame-baseline` |
| Workflow | `gen_animation_ltx23_by_url.json` |
| Checkpoint | `ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors` |
| Frames / FPS | 97 / 24 |
| Duration | 4.041667 seconds |
| Steps | 8 |
| CFG | 1.0 |
| Sampler | `euler_ancestral_cfg_pp` |
| Seed | 9221001 |
| Conditioning | first source frame plus text prompt; no driving-video control |

Manual scores use a provisional 0–5 scale: 5 means the sampled frames closely
meet the case requirement, 0 means the required behavior is reversed or
absent, and `N/A` means the source does not expose that property. These scores
are review evidence, not an automatic acceptance gate.

## Results

### 1. `walk-and-address-camera`

Task: `8758e460-1470-4031-8431-50b30a25119c`<br>
Generated artifact: https://autorig.online/renderfin/render/default_user/8758e460-1470-4031-8431-50b30a25119c.mp4<br>
Local generated SHA-256: `626d6d8e22ef98bea308768db4a59aba3a6f64ddfac07cc252d1b25617131dc1`

Requested output was 540×960. ffprobe verified H.264, 540×960, 24 FPS,
97 frames, and 4.041667 seconds. The source segment is H.264, 270×480,
30 FPS, 120 frames, and 4.000000 seconds. The generated comparison is H.264,
960×270, 192 frames, and 4.000000 seconds; it is an inspection derivative,
not a delivery artifact.

Visible result:

- One man remains present. The bucket hat, dark glasses, beard, red shirt,
  bright sky, palm leaves, and low selfie angle remain recognizable.
- The face stays broadly recognizable across the sampled frames, although the
  mouth opens much wider and the speaking expression becomes more theatrical
  than in the source.
- The generated hand gesture is larger and follows a different trajectory.
  It is related to the prompt but does not reproduce the sampled source motion.
- No clearly extra person or clearly detached limb is visible in the samples.
  Several hand frames are motion-blurred or partly cropped, so exact finger
  anatomy cannot be certified from these sheets.
- The first-frame watermark and QR-like mark remain visible. This is faithful
  to the input pixels but undesirable for a clean production source.

| Criterion | Sample score / 5 |
|---|---:|
| Face quality | 4 |
| Anatomy | 3 |
| Identity continuity | 4 |
| Action fidelity | 2 |
| Principal person count | 5 |
| Camera/composition fidelity | 4 |
| Temporal continuity | 3 (sampled only) |

Disposition: **candidate only for loose selfie B-roll after retuning**, not a
validated reenactment pipeline. First-frame conditioning is adequate for broad
appearance and composition here. A driving-video/control path is needed when
the hand trajectory and camera behavior must match the reference.

### 2. `seated-speaking`

Task: `4cf7270f-5e8e-4d69-9f24-a951e514517f`<br>
Generated artifact: https://autorig.online/renderfin/render/default_user/4cf7270f-5e8e-4d69-9f24-a951e514517f.mp4<br>
Local generated SHA-256: `7fe2925604fd567a0082f61012010d17d060b5469cc6c45cb595ea628d3b7ab6`

ffprobe verified the generated artifact as H.264, 960×540, 24 FPS,
97 frames, and 4.041667 seconds. The source segment is H.264, 480×270,
approximately 59.94 FPS, 240 frames, and 4.004000 seconds. The comparison is
H.264, 960×270, 335 frames, and 4.000000 seconds.

Visible result:

- One seated woman, her short curly hair, striped shirt, frontal framing, and
  plain wall remain stable in the samples.
- The face is consistently recognizable and does not show an obvious sampled
  collapse. Expression timing differs: the generated smile and open mouth are
  broader and persist longer.
- The generated clip contains the requested broad sequence: speaking, open
  two-hand gesture, and hands moving toward the chest. The timing and exact
  hand paths differ from the source.
- Both hands remain associated with the subject. Some fingers are blurred,
  cropped, or too small in the sheets for a reliable digit count; there is no
  obvious extra arm in the sampled frames.
- The static camera, person count, shirt pattern, and background are the
  strongest of the three reviewed cases.

| Criterion | Sample score / 5 |
|---|---:|
| Face quality | 4 |
| Anatomy | 3 |
| Identity continuity | 4 |
| Action fidelity | 4 for broad semantics; 2 for exact trajectory |
| Principal person count | 5 |
| Camera/composition fidelity | 5 |
| Temporal continuity | 3 (sampled only) |

Disposition: **best baseline candidate of the first three for prompt-led,
low-constraint speaking footage**, pending continuous playback review. Use
driving-video/control conditioning when gesture ordering, hand path, or speech
timing must follow a specific performance.

### 3. `single-hand-gesture`

Task: `1c42361c-3e40-407b-8438-a4e79438b134`<br>
Generated artifact: https://autorig.online/renderfin/render/default_user/1c42361c-3e40-407b-8438-a4e79438b134.mp4<br>
Local generated SHA-256: `7b280543058fa8eb5f4f295982a72b56caad5915c5f77441ae9a8aaf87cfbd29`

ffprobe verified the generated artifact as H.264, 960×540, 24 FPS,
97 frames, and 4.041667 seconds. The source segment is H.264, 480×270,
approximately 29.97 FPS, 120 frames, and 4.004000 seconds. The comparison is
H.264, 960×270, 215 frames, and 4.000000 seconds.

Visible result:

- The source sustains a thumbs-down gesture. The generated clip changes it to
  a clear thumbs-up gesture and holds that reversed meaning for nearly all
  sampled frames.
- The generated forearm and hand are visually plausible in many static
  samples, but plausible anatomy does not compensate for the reversed action.
- The arm enters/reconstructs differently from the source and its scale and
  position change. The neutral wall and single-arm count remain broadly stable.
- No face or full body exists in the source, so face and identity scores are
  not applicable.

| Criterion | Sample score / 5 |
|---|---:|
| Face quality | N/A |
| Anatomy | 3 |
| Identity continuity | N/A |
| Action fidelity | 0 |
| Principal limb/person count | 4 |
| Camera/composition fidelity | 3 |
| Temporal continuity | 2 (sampled only) |

Disposition: **reject this first-frame pipeline for directional gestures**.
The prompt explicitly requested thumbs-down and the source first frame already
contained thumbs-down, yet the output reversed the gesture. Increasing prose
alone is not a credible fix. This case should be rerun through the actual
driving-video/control workflow; a first-plus-last-frame constraint can be a
secondary test but does not encode the full motion path.

## Decision for the next benchmark cases

The first-frame baseline should remain in the matrix as a cheap appearance and
composition baseline. It should not be the only pipeline considered for the
user's “same plot and action” requirement.

Recommended control allocation:

1. Use first-frame + prompt as the baseline for seated speaking and simple
   close-up B-roll. It is currently strongest where the requested action can be
   described broadly and exact timing is unimportant.
2. Add driving-video/control conditioning for directional hand gestures,
   walking, prop contact, multi-person interaction, and any scene whose action
   meaning depends on trajectory. `single-hand-gesture` is the clearest hard
   failure demonstrating this need.
3. Keep face/identity conditioning separate from motion control. The reviewed
   clips used only the source first frame; they do not validate a reusable
   Avatar identity across new scenes.
4. Prioritize the next controlled comparisons on `seated-speaking`,
   `single-hand-gesture`, then `two-person-cafe-conversation`. They isolate,
   respectively, face-plus-gesture quality, directional action fidelity, and
   two-person identity/count preservation.
5. Treat `rear-view-walk` mainly as camera/architecture stress and
   `two-person-walk` mainly as gait/occlusion/prop stress. Their source views do
   not expose faces well enough to qualify Avatar identity.

No reviewed case is marked “validated.” `seated-speaking` is a candidate,
`walk-and-address-camera` needs motion retuning, and `single-hand-gesture` is
rejected for action fidelity under the first-frame-only configuration.
