# /how-it-works — V3 refresh (agent B, 2026-10-16)

File: `static/how-it-works.html` (en/ru/zh/hi/fa through i18n). Live page vs the local file before the work: identical
except the server injection (header/footer, canonical, hreflang, social tags, `?v=` stamps), so the local file was the
right starting point.

## What changed
Whole page rewritten to the V3 flow; OpenPose, T-pose preparation, Unity HDRP, video preview and roadmap are out.
1. Hero: kicker, h1 (existing key), lead, two CTAs, four fact tiles (1 min target, 9 categories, 1 viewer, 5 languages),
   strip of five real posters. «On this page» anchor row (crawlable).
2. `#flow`: seven steps (upload anything, fast rig, clips, textures, background checks, viewer, export), icon timeline,
   each step has an id (`#step-1`..`#step-7`) used by the HowTo JSON-LD; horse clip GIF in step 3.
3. `#inside`: four real rig outputs (rider weights, bird weights, horse gallop frames, hand clips).
4. `#viewer`: black-first load, six features (blueprint B / 15 s re-rig, camera presets, hotkeys + mouse, shared
   session agent in the creator's language, offered skeleton confirmation, own clips), four scene renders.
5. `#categories`: nine cards (biped incl. primate + reference-leg rule, quadruped, equine, bird, serpent, monster,
   multilimb, hands, vehicle), one short paragraph each.
6. `#vehicles`: input / segmentation map / wheels cut / bare shell, the isolated-parts sheet, seven stages.
7. `#quality` (regression tests, release gates, numeric checks, Telegram defect buttons), `#export` (GLB, FBX, USDZ,
   ZIP, Unity package, Blender; plugin and API buttons), `#related` (15 crawlable internal links), CTA.
No prices on this page; no «V2», OpenPose, Mixamo, Unity/Blender version numbers or «coming soon».

## SEO
title (51): «How AutoRig Works: Auto Rig Any 3D Model in Seconds»; description (147); keywords; canonical;
hreflang en/ru/zh/hi/fa/x-default (the server rewrites them per language); og:* + twitter:* with
`static/images/og/how-it-works.jpg` 1200x630 (made from real posters); JSON-LD WebPage + HowTo (7 steps); one h1.
Localized title/description come from `seo_hiw_title` / `seo_hiw_description` (zh and hi had none before).

## i18n (static/i18n/pending/hiw.{en,ru,zh,hi,fa}.json, 149 keys each, same key set)
New: all `hiw_*` keys used by the page except the two reused unchanged: `hiw_hero_title`, `hiw_cta_button`.
Changed text (overrides, expected by the merge): `hiw_meta_title`, `hiw_hero_lead`, `hiw_cta_title`, `hiw_cta_subtitle`.
Added SEO keys: `seo_hiw_title`, `seo_hiw_description` (override ru/fa/en, new in zh/hi).
Dropped (48, used by no other file; lead removes with `i18n_merge.py --write --drop`):
hiw_product_title,hiw_product_desc,hiw_product_highlight,hiw_formats_title,hiw_formats_desc,hiw_openpose_title,
hiw_openpose_desc,hiw_markers_body,hiw_markers_hands,hiw_markers_face,hiw_openpose_humanoid,hiw_tpose_title,
hiw_tpose_intro,hiw_tpose_orientation,hiw_tpose_scale,hiw_tpose_limbs,hiw_tpose_fingers,hiw_tpose_result,
hiw_skeleton_title,hiw_skeleton_intro,hiw_skeleton_create,hiw_skeleton_create_desc,hiw_skinning_auto,
hiw_skinning_auto_desc,hiw_skeleton_result,hiw_animations_title,hiw_animations_intro,hiw_baked_title,hiw_baked_desc,
hiw_controlrig_title,hiw_controlrig_desc,hiw_animations_result,hiw_unity_title,hiw_unity_intro,hiw_unity_1,
hiw_unity_2,hiw_unity_3,hiw_unity_result,hiw_video_title,hiw_video_intro,hiw_video_1,hiw_video_2,hiw_video_3,
hiw_video_result,hiw_roadmap_title,hiw_roadmap_1,hiw_roadmap_2,hiw_roadmap_3

## Images (static/images/how-it-works/, all <= 300 KB, width/height set, below-fold lazy)
* hero-1..5, cat-{biped,quadruped,equine,bird,serpent,monster,multilimb,hands}: gallery posters via `/thumb/<task>`:
  man = task ab10946e, wolf 50a9139d, zebra 62c7f4cd, parrot 798eb820, monster 268bd20c, spider 6a490b56,
  hand 90691082, worm-like creature 3a099711 (serpent card).
* cat-vehicle, veh-input/map/cut/shell/isolated-sheet: v3 pool, motorhome run c91d4e7f (task c651e1f4).
* inside-rider-weights (run ede89a95), inside-bird-weights (run 48bc7952), inside-horse-gait (run 028d1acd),
  inside-hand-clips (run d3ef5ccb), clip-horse.gif (run 75550318): v3 pool.
* scene-{cathedral,river-glade,sponza,jungle-ruins}: unity scene-media review renders (via the v3 pool).
* `static/images/og/how-it-works.jpg`: collage of five hero posters + title (Pillow).

## Checks (script: scratchpad hiw/check.py, run on the file)
* html.parser: no unclosed or mismatched tags; h1 count 1; 28 images, all with alt, width, height.
* every `/static/...` src exists on disk; 16 internal hrefs answer 200 on autorig.online (curl HEAD); all `#anchors` exist.
* JSON-LD WebPage and HowTo parse; CSS braces 100/100; no JS touched (no node check needed).
* every `data-i18n` key (149) exists in all five pending files; same key set in the five files; page English == en pending.
* `tools/i18n_merge.py --page hiw` (dry run): ok, en +145 ~4, ru +143 ~6, zh/hi +145 ~4, fa +143 ~6.
* `user_language.translate_html` with merged dicts: no untranslated visible English left in ru/zh/hi/fa; titles localized.
* Latin leftovers in ru/zh/hi/fa only technical terms (GLB, FBX, PBR, SAM2, Qwen-image, IK, FFD, Unity, Blender).
* Not done (rules): no browser render, so the 375 px layout and dark/light look are checked by CSS design only
  (grids collapse at 992/768/576 px, logical properties for RTL); please eyeball `/how-it-works` and `/fa/how-it-works`.

## Open questions for the lead
1. `<img alt>` is English only (data-i18n has no alt attribute); captions are localized. Add `data-i18n-alt` to the
   shared i18n if wanted.
2. JSON-LD is English on every language page (the server skips `<script>` blocks).
3. The serpent card uses a worm-like creature (the only limbless poster in the gallery); swap it when a real snake
   task exists. The parrot is a cartoon biped bird; replace with a real bird rig screenshot if you have one.
4. Still missing from the pool: viewer screenshots of the blueprint side view, the Parts panel and the agent chat;
   the page would take them in `#viewer` and `#vehicles`.
5. Related links point to `/dog-auto-rig`, `/horse-auto-rig`, `/t-pose-rig`; agents C and D change those pages, the
   labels are generic on purpose.
