# /t-pose-rig — site refresh to V3 (agent D, 2026-10-16)

Files: `static/t-pose-rig.html` (path `/t-pose-rig`), `static/i18n/pending/tpose.{en,ru,zh,hi,fa}.json` (67 keys each),
`static/images/t-pose-rig/*.jpg`, `static/images/og/t-pose-rig.jpg`. Started from the live HTML (`curl -s
https://autorig.online/t-pose-rig`): identical to the old local file apart from the server-injected parts.

## What changed
The old page said the T-pose is the industry standard and «Recommended», had a Service JSON-LD with an offer and a
«T-pose vs A-pose» table. It is now an article with the V3 truth (spec §1 and the 2026-10-15 reference-leg rule):
1. Hero: «T-pose rig: any pose works», three facts (any pose accepted, bind pose from the file first, skeleton
   confirmation never blocks), CTA.
2. «Do you need a T-pose to rig a character?» No; fast rig in seconds; target one minute from pickup.
3. «What AutoRig does with each pose»: 5 cases (T/A-pose, other standing pose, skeleton or animation in the file,
   feet not side by side, not a biped).
4. «How a posed model is rigged»: 4 steps (file checked first -> bind pose; fast rig; reference leg; skeleton offered,
   not demanded).
5. «The standing leg is the reference»: the rule in three paragraphs + an inline SVG schematic (L1 hip-knee, L2 knee-ankle,
   L3 ankle-toe; reference leg vs traced leg). The schematic is drawn, not a screenshot, and is labelled «Schematic».
6. Blueprint mode (B, symmetry, re-rig after 15 s or by the button).
7. «Rigged results from V3»: 4 figures (biped, bird, horse, hand) with captions.
8. «Not everything has a T-pose» + links (animal rigging, horse, dog, guides), FAQ (6 questions), related links, CTA.
Removed from the old text: «Perfect T-pose», «Industry standard», IK-bone and Maya/Rigify compatibility claims, the
«T-pose recommended / A-pose limited» table, the Service offer.
Page-local `<style>` (prefix `tp-`, logical properties for RTL). `app.js` is kept (it runs `I18n.init`, auth, credits).
A small inline script sets `lang`/`dir` on `<main id="tp-main">` because `/t-pose-rig` is a chrome-scope page (see questions).

## Images
- rider-weights.jpg: run ede89a95 / task b13b5a46 (biped T-pose, skeleton + weights). bird-sheet.jpg: run 48bc7952.
  horse-sheet.jpg: run 028d1acd. hand-sheet.jpg: run d3ef5ccb. All from `static/images/v3/`, 640x480, 25-34 KB.
- og/t-pose-rig.jpg 1200x630 (62 KB): rider + bird weights + horse sheet + wordmark.
- No gallery posters here: the ones with a pose were third-party characters.

## SEO
title (51): «T-Pose Rig: AutoRig Rigs Any Pose, No T-Pose Needed» (keyword «T-pose rig» first). description (152),
keywords (t pose rig, T-pose rigging, rig any pose, bind pose ...), canonical, og:type article, og/twitter image + alt, one h1.
JSON-LD `@graph`: Article (dateModified 2026-10-16, publisher with logo), FAQPage (6 Q/A), BreadcrumbList; valid JSON.
`seo_tpose_title` and `seo_tpose_description` are in the pending files for the day `/t-pose-rig` gets an SEO key.
hreflang not added (see questions).

## i18n
New keys (67): `seo_tpose_*`, `tpose_h1|lead|cta|f1-3`, `tpose_answer_*`, `tpose_cases_h2`, `tpose_c1-5_t|d`, `tpose_how_h2`,
`tpose_s1-4_t|d`, `tpose_leg_*` (h2, p1-3, ref, traced, lengths, note, svg_title), `tpose_bp_*`, `tpose_media_h2`, `tpose_cap1-4`,
`tpose_other_*`, `tpose_faq_h2`, `tpose_q1-6`, `tpose_a1-6`, `tpose_related_*`, `tpose_cta_*`. Reused existing keys:
`nav_guides`, `footer_tpose`, `footer_how_it_works`, `footer_animal_rig`, `footer_glb|fbx|obj_rig`, `footer_faq`,
`nav_gallery`, `guides_card_horse_title`, `guides_card_dog_title`. Dropped: none (the old page had no keys). `tpose_notice` (home
page, «Models must be in T-Pose for best results») is the V2 claim to fix on index.html.

## Checks (same scripts as guides)
- parser: no unclosed tags, 1 h1, 7 imgs with alt/width/height (3 decorative icons have `alt=""`), no duplicate ids -> OK
- 26 refs: all `/static` files on disk, every page 200 -> OK. JSON-LD parses (Article, FAQPage, BreadcrumbList) -> OK
- title 51, description 152; all five languages inside 60/160 -> OK
- 76 `data-i18n` keys: all in the pending files or shared JSON, identical key sets, every value in its own script -> OK
- backend `translate_html` for ru/zh/hi/fa: all keys replaced -> OK; `node --check` of both inline scripts -> OK
- Headless Chrome copies: 1280 dark, 375 px light/Persian RTL: no horizontal overflow (scrollWidth 375), RTL text and numbered
  cards flip correctly, the SVG stays left-to-right.

## Open questions for the lead
1. `/t-pose-rig` is not in `FULL_PAGE_PATHS` (user_language.py and i18n.js), `ADVERTISED_PATHS` or `SEO_KEYS`. Add it to all
   three (`"/t-pose-rig": "tpose"`) to get a real RTL page for fa, translated title/description and the hreflang cluster; until
   then only the body text is translated and the `<main>` direction comes from my inline script.
2. Alt texts of images are English only (no `data-i18n-alt`).
3. Two statements go beyond spec §1 and come from the project rules of 2026-10-15: symmetry follows the voxels (mirror IoU) and
   the rule that an asymmetric model keeps its own bones in both legs. Remove the paragraph `tpose_leg_p3` if you do not want them.
4. A real blueprint screenshot of a posed model (Vegeta 1ceccd8a, the asymmetric corpus case) would replace the schematic.
5. `/t-pose-vs-a-pose` still carries the old copy (round 2).
