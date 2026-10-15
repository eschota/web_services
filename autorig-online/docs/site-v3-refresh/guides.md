# /guides — site refresh to V3 (agent D, 2026-10-16)

Files: `static/guides.html` (path `/guides`), `static/i18n/pending/guides.{en,ru,zh,hi,fa}.json` (56 keys each),
`static/images/guides/*.jpg`, `static/images/og/guides.jpg`. The page was started from the live HTML
(`curl -s https://autorig.online/guides`): it equals the old local file except the server-injected header, footer,
canonical and social tags, so nothing in production was ahead of the checkout.

## What changed (sections)
1. Hero: new V3 h1 and lead (any input, rig in seconds), two buttons (Rig a model, How it works), three fact chips.
2. NEW «Guides by what you rig»: 7 cards. Humanoids/bipeds -> /humanoid-auto-rig, Quadrupeds -> /dog-auto-rig,
   Horses/deer/zebras -> /horse-auto-rig, Birds and wings -> /animal-rig, Hands/gloves/FPS arms -> /how-it-works,
   Cars from one picture -> /how-it-works, Monsters/serpents/multi-limb -> /animal-rig (wide card).
3. NEW «Inside the viewer»: 6 cards (blueprint mode, camera presets and hotkeys, session agent, Parts panel of a car,
   environment scenes, textures and downloads) -> /how-it-works or /faq.
4. The 13 rig-type cards (humanoid ... pig) are unchanged, same keys (all 13 pages answer 200).
5. Workflow tutorials: copy rewritten to V3 (Mixamo, Unity, Unreal, T-pose vs A-pose, GLB vs FBX, OBJ, retargeting,
   image to character); face-rig card kept as is; NEW card «T-pose rig: any pose works» -> /t-pose-rig.
6. Related links row (GLB/FBX/OBJ, animal rigging, how it works, FAQ, gallery, Blender plugin, API) and a CTA.
Removed: the old card that reused the page H1/subtitle keys of the image-to-character page (`img2rig_h1`, `img2rig_subtitle`).
Page-local `<style>` (prefix `gx-`), no shared CSS touched; header/footer left to the server injection.

## Images (all real V3, from `static/images/v3/` unless noted; 640x480 JPEG q82, 15-54 KB)
- cat-biped: rider-weights.png (task b13b5a46, run ede89a95). cat-equine: horse 028d1acd sheet. cat-bird: run 48bc7952 sheet.
- cat-hands: run d3ef5ccb sheet. cat-vehicle: car-motorhome-input (run c91d4e7f). view-parts: top row + bottom row of
  car-motorhome-isolated-sheet (same run). view-scenes: scene-cathedral + scene-river-glade.
- cat-quad: three gallery posters `/thumb/` of tasks 8486c5fe (wolf), 51e6c514 (goat), f5a69d34 (dog); public tasks,
  two are `needs_review`. Swap for a pool image if you prefer.
- og/guides.jpg 1200x630 (103 KB): motorhome + three rig check strips (horse, bird, hand) + logo and wordmark.
Blueprint, camera, agent and textures cards use site icons (no screenshots yet).

## SEO
title (52): «AutoRig Guides: Rig Characters, Animals, Hands, Cars». description (154), keywords, robots (max-image-preview),
canonical `https://autorig.online/guides`, og:* and twitter:* (image 1200x630 + alt), one h1, h2 per section.
JSON-LD `@graph`: CollectionPage with an ItemList of 26 guide URLs + BreadcrumbList (valid JSON).
`seo_guides_title` / `seo_guides_description` are in the pending files because `user_language.py` swaps title and
description from those keys on `/ru|zh|hi|fa/guides`.
hreflang: NOT added (see open questions).

## i18n
New keys (56): `seo_guides_*`, `guides_h1`, `guides_lead`, `guides_hero_cta`, `guides_fact1-3`, `guides_cat_*`
(h2, intro, 7 x _t/_d), `guides_link_see|animals|faq`, `guides_view_*` (h2, intro, 6 x _t/_d), `guides_wf_*_d` (8),
`guides_wf_tposerig_t`, `guides_related_*`, `guides_cta_*`. Reused existing keys: `guide_*_title`, `guide_face_rig_*`,
`footer_image_to_rig`, `guides_card_*`, `guides_card_read`, `guides_rig_types_*`, `guides_workflow_*`, `read_guide`,
`footer_how_it_works`, `footer_glb|fbx|obj_rig`, `footer_animal_rig`, `footer_faq`, `nav_gallery`, `nav_blender_plugin`, `nav_api`.
Unused after this change (grep shows only guides.html used them; check again before deleting): `guides_title`, `guides_intro`,
`guide_mixamo_desc`, `guide_unity_desc`, `guide_unreal_desc`, `guide_tpose_desc`, `guide_formats_desc`, `guide_obj_desc`,
`guide_retargeting_desc`, `img2rig_h1`, `img2rig_subtitle`.

## Checks (scratchpad `agentD/check.py`, `preview.py`, `mobile.py`)
- parser: no unclosed or mismatched tags, 1 h1, 20 imgs all with alt/width/height, no duplicate ids -> OK
- 54 local/internal refs: every `/static/...` file exists on disk, every page answers 200 on autorig.online -> OK
- JSON-LD parses; title 52 <= 60, description 154 <= 160 (all five languages inside the limits) -> OK
- 105 `data-i18n` keys on the page: all present in the 5 pending files or the 5 shared JSON; the pending sets have identical
  keys; no value left in English, each language is in its own script -> OK
- `translate_html` of the backend run on the page for ru/zh/hi/fa: every key replaced -> OK
- `node --check` on both inline scripts -> OK (no external JS touched)
- Headless Chrome on copies (not the browser pane): 1280 dark, 1280 light Persian RTL, 375 px iframe: scrollWidth 375 =
  clientWidth 375, 0 overflowing elements. Header and footer are not in these copies.

## Open questions for the lead
1. hreflang: `/guides` is in `FULL_PAGE_PATHS` and `SEO_KEYS` but not in `ADVERTISED_PATHS` (user_language.py), so `/ru/guides`
   is `noindex` with canonical to English. Add `"/guides": "guides"` to `ADVERTISED_PATHS` to get the hreflang cluster.
2. Missing guide pages (cards link to /animal-rig, /how-it-works, /faq instead): vehicles (cars from a picture), hands and gloves,
   birds and wings, quadrupeds as one category, blueprint bone editing, the session agent, environment scenes, textures.
3. Viewer screenshots wanted: blueprint side view, Parts panel, session agent chat, hand rig in the viewer.
4. `img alt` text stays English (the i18n layer has no `data-i18n-alt`).
5. The 13 rig-type pages are generated by the backend; their copy was not checked for V2 wording.
6. `tpose_notice` («Models must be in T-Pose for best results») is still on the home page (agent A).
