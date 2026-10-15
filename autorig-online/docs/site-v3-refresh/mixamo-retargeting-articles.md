# Articles: mixamo-alternative and animation-retargeting x (en, ru, zh, hi) (agent B, 2026-10-16)

Files: `static/mixamo-alternative{,-ru,-zh,-hi}.html`, `static/animation-retargeting{,-ru,-zh,-hi}.html` (8 files).
Start: the live HTML of all eight equals the local files plus server injection (header/footer, canonical, hreflang,
social tags, `?v=` stamps). Finding: in the old ru/zh/hi copies only the hero, title and description were translated;
the body was English and the zh/hi descriptions and JSON-LD stayed English. All eight are now fully rewritten.

## What changed
* One generator per family (scratchpad `hiw/art/gen.py`, `mixamo.py`, `retarget.py`) writes the four copies from one
  structure, so the ru/zh/hi copies have exactly the same blocks as the English one (checked: 67 and 39 blocks equal).
  The language copies are real translations written for each language, not machine leftovers.
* **mixamo-alternative** (V3): any model, any pose (no T-pose requirement, reference-leg rule, rest pose from the file),
  nine rig categories with a six-card image strip, the vehicle conveyor in brief, clips at once, the viewer with blueprint
  mode (B, 15 s re-rig, camera presets, offered skeleton confirmation, shared session agent), exports (GLB, FBX, USDZ,
  ZIP, Unity package, Blender), steps, when it makes sense, FAQ (free try, $20/month Unlimited Monthly as on the live
  buy page, T-pose, non-human, manual rigging).
* **animation-retargeting** (V3): clip library per category made from generated reference videos (extracted once,
  retargeted automatically onto every new rig, procedural clips as fallback), the fit / retarget / check / show flow
  with numeric QA per clip, why retargeting looks bad and what is done about it, horse clip GIF + gallop frames + hand
  and bird clip sheets, custom clips in the viewer clip set, exports, Unity Humanoid and Unreal notes, FAQ.
* Removed: V2 / OpenPose wording, «T-pose required», «50+ animations», «Preview video», the old generic Unity/Unreal text
  that implied a T-pose pipeline.
* Hero blocks keep the site classes (`hero`, `content-section`, `related-section`, `cta-section`); a small page-local
  `<style>` adds figures, callout and category list (logical properties, 3/2/1 columns at 768/480 px).

## SEO per copy
Own `<html lang>`, title (en 51/54, ru 51/52, hi 49/50 chars; zh 24/29 characters), description (<= 160, zh 73/76
characters), keywords in the language, canonical of the copy, hreflang set en/ru/zh/hi + x-default (same in all four),
og:* and twitter:* with a per-language image `static/images/og/<slug>[-ru|-zh|-hi].jpg` (1200x630; the hi image uses
Latin text because Pillow here has no complex-script shaping), one h1, JSON-LD Article (inLanguage, image,
dateModified 2026-10-16) + BreadcrumbList (Home, Guides, article) as two scripts. No i18n keys.
Internal links per copy: related-guide buttons (10 and 9), in-text links to the pose guide, animal pages, vehicle
section of /how-it-works, retargeting/mixamo siblings in the same language, Unity/Unreal/GLB-vs-FBX guides in the same
language, the Blender plugin, pricing.

## Images (static/images/articles/, all <= 100 KB)
mixamo-cat-{biped,quadruped,equine,bird,hands,vehicle}.jpg (gallery posters / vehicle input, same sources as
how-it-works: tasks ab10946e, 50a9139d, 62c7f4cd, 798eb820, 90691082; motorhome run c91d4e7f),
mixamo-rider-weights.jpg (run ede89a95), retarget-horse-gait.jpg (run 028d1acd), retarget-horse-clip.gif (run 75550318),
retarget-hand-clips.jpg (run d3ef5ccb), retarget-bird-clips.jpg (run 48bc7952). All `<img>` have width, height, alt in the
language of the copy, lazy-loaded (the pages have no above-the-fold photo).

## Checks
* `py -3.11 tools/site_page_check.py <file>` on each of the 8 files: RESULT OK (h1 1, no unclosed tags, no image
  warnings, canonical and 5 hreflang entries present, JSON-LD valid, no V2/OpenPose/coming-soon wording).
* Own script: titles, descriptions, `lang`, 16 JSON-LD blocks parse, all `/static/` images exist, every `<img>` has
  alt/width/height, 37 distinct internal links answer 200 on autorig.online, block structure of ru/zh/hi equals en.
* Not done (rules): no browser render; layout relies on the shared article CSS plus the small local block.

## Open questions for the lead
1. Mixamo is described only as «services built around humanoid characters in a standard pose»; I made no further
   claims about Mixamo. Tell me if the owner wants a stricter comparison.
2. The FAQ states the live $20/month plan and a free try (both from the live pages); drop them if pricing text should
   stay off articles.
3. `animal-rig` and `horse-auto-rig` have no -ru/-zh/-hi copies, so those links stay unprefixed (the server rewrites
   main-page links only).
4. A screenshot of the blueprint side view would help the mixamo article; none exists in the pool yet.
