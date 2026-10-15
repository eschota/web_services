# /image-to-rigged-3d-character[-ru|-zh|-hi] — V3 refresh (agent B, 2026-10-16)

Files: `static/image-to-rigged-3d-character.html` and the `-ru`, `-zh`, `-hi` copies.

## Starting point (important)
The local checkout was stale again: live no longer sells credit packs here. Live has the Unlimited Monthly wording
(`$20/month`, «Existing credit balances remain valid»), a changed account FAQ answer and a «Unlimited Monthly» related link;
the local files still said «3 credits», «10 credits», «$0.10 per credit», «refund». I rebuilt from the live HTML of all four
copies. The `#pricing`, `#batch` and `#examples` sections and the account/«several at once»/«need a 3D model» FAQ
answers are reused byte for byte from the live copies (script-verified identical), so every price and plan fact is exactly
as live, in all four languages. The live ru/zh/hi were already real translations; the new text is translated the same way.

## What changed
* Flow: picture -> 3D model on the fleet (paid cloud generators Tripo / Hunyuan cloud only on the person's button) ->
  textures (own kept, PBR when missing) -> the V3 rig with clips -> one viewer. Five steps with ids `#step-1`..`#step-5`
  (HowTo JSON-LD uses them). A picture of a car goes to the vehicle conveyor: new `#car` section with the real motorhome
  run (input, segmentation map, wheels cut, bare shell, isolated-parts sheet) and five bullets.
* Removed: «T-pose or A-pose required» framing (T/A pose now «cleanest, not required», rig fitted as is), «20 to 50 minutes»
  and HowTo `totalTime`/`estimatedCost` (no generation time or price is promised), HDRP unitypackage and preview-video
  cards, «9 to 17 artifacts». Downloads now: GLB, FBX with clips, USDZ, Unity package, Blender file, ZIP.
* FAQ rewritten (T-pose, how long without a number, not a character, animals/birds/hands, paid AI) + FAQPage JSON-LD.
* Examples: the two good videos (pink creature, dwarf) stay; knight and bear shaman are removed because their posters
  are broken white geometry (old-pipeline output); VideoObject graph trimmed to the same two.
* New `art-figs` styles in the page `<style>`; the old `img2rig-*` styles and the language-switch script are unchanged.

## SEO per copy
`<html lang>`, title (en 56, ru 60, hi 52 chars; zh 30 characters), description (<= 160; zh 58 characters), keywords,
canonical, hreflang en/ru/zh/hi + x-default, og/twitter with `static/images/og/image-to-rigged-3d-character[-ru|-zh|-hi].jpg`
(1200x630; hi image has Latin text, no complex-script shaping in Pillow), one h1, JSON-LD TechArticle + BreadcrumbList
(Home, Guides, article) + HowTo + VideoObject graph + FAQPage. Internal links: /, /how-it-works (+#vehicles), /faq, /guides,
/gallery, /animation-retargeting, /mixamo-alternative, /animal-rig, /glb-auto-rig, /fbx-auto-rig, /buy-credits (ru/zh/hi
use the same-language sibling pages where they exist).

## Images (static/images/articles/)
i2c-car-input / map / cut / shell / parts (motorhome run c91d4e7f, v3 pool); videos and posters are the existing
`static/videos/image-to-rig/pink-creature*`, `dwarf*`. OG collage: pink creature, dwarf, parrot (task 798eb820), wolf
(50a9139d), motorhome.

## Checks
* `PYTHONIOENCODING=utf-8 py -3.11 tools/site_page_check.py <file>` on the four files: RESULT OK (h1 1, JSON-LD valid,
  no V2/OpenPose/coming-soon wording, hreflang set complete).
* Own script: structure of ru/zh/hi equals en (114 blocks), all images/posters exist, every `<img>` has alt/width/height,
  21 internal links answer 200, no time or per-credit price claims left, pricing and batch sections identical to live.
* Not done (rules): no browser render.

## Open questions for the lead
1. The two kept videos are August (pre-V3) previews. If a V3 image-to-model example exists, swap the grid for it.
2. The pricing text still says «Free: running the rig on an uploaded 3D model needs no account» exactly as live; nothing changed.
3. The task links `/task?id=...` of the two examples will re-rig on open under the current rule; their previews stay old.
