# LTX overlapping guide latent crop

The identity `LTXVAddGuide` and video `LTXAddVideoICLoRAGuide` both begin at
pixel frame 0. Both append encoded guide latents to the generated latent.
`LTXVCropGuides`, however, removes the number of unique temporal coordinates in
`keyframe_idxs`. The shared frame-zero coordinate is counted once, so one
identity latent remains at the end of the sequence.

For a 97-frame request the generated latent has 13 temporal entries. Before the
fix, the remaining identity latent made 14 entries and the VAE decoded 105
frames. Pixel-space trimming returned 97 frames, but frame 96 already contained
temporal bleed from the trailing frame-zero identity latent. The public seated
Pose result visibly reset toward frame 0 in its last frame.

The fix inserts `LTXVSelectLatents(start_index=0, end_index=12)` between
`LTXVCropGuides` and `LTXVTiledVAEDecode`. A 25-frame request uses end index 3.
The existing decoded-frame trim remains as an exact-delivery safeguard.

Focused validation:

- `test_render_runtime_settings.py`: 8 passed.
- 97-frame graph: decoder consumes `delivery_latents_decode_video`, end index 12.
- 25-frame graph: end index 3.
- Plain image-to-video graph: unchanged; no selector inserted.
- Control-video cache namespace bumped independently so prior affected outputs
  are not reused.
