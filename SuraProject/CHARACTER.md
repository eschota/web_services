# Sura — character canon

One detail per line, matching the text nodes of the pose-library graph, so a
detail changed here is changed there by the same words.

## Identity

- Name: Sura. Adult (21+), fictional, AI-generated.
- Personality for YouTube: playful, teasing, a gamer, a bit chaotic, warm with
  viewers. Not sexual on YouTube.

## Look (SFW parts — usable on YouTube)

| Key | Canon text |
|---|---|
| face | high cheekbones, a small straight nose, full lips, dark shaped eyebrows, long lashes, soft golden-olive skin |
| eyes | heterochromia: as seen in the picture, the eye on the left is emerald green, the one on the right deep blue; natural, no glow |
| septum | a small thin silver septum ring |
| hair | long straight black hair, middle parting, two small pink hair clips above the temples |
| jewellery | thin black choker, small hoop earrings, two dark bracelets on the left wrist |
| body | slim waist, wide hips, long legs |
| tattoo_bows | a small black ribbon bow on each forearm |

## Look (adult canon — never on YouTube)

Chest heart, belly heart, a thin dragon ring around the right thigh, a lace
garter band on the left thigh, angel wings across the shoulder blades, a few
coloured hearts with sparks on the buttocks, intimate details. Full wording:
graph `5a7b0c19e4d2`, nodes `a_*`.

## References

- Source portrait (adult, not for YouTube):
  `https://autorig.online/renderfin/render/default_user/51580536-8dfe-4d10-8d22-4fb4ed16782a.png`
- Tattoo plates, verified with Vision 2026-09-30: front
  `…/f9bfc6a6-8d4f-4b95-ad47-f249514f7c29.png`, back
  `…/f4636189-f637-40d1-9223-0108ed50c043.png` (adult).
- **Needed:** a clothed SFW reference sheet (front, 3/4, side) for YouTube work.

## Known model weaknesses

- The dragon does not wrap all the way around the thigh (three wordings tried).
- Naming an out-of-view tattoo makes Qwen draw it on the visible side: give each
  view only the details it can show.
- Qwen edit treats `<image1>` as the canvas: a standing plate there pulls every
  pose towards standing/frontal.
