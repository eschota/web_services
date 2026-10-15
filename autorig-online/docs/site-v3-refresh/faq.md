# /faq refresh to V3 (page agent E, 2026-10-16)

Files: `static/faq.html` (started from the live HTML: the local file equalled live apart from the injected header/footer,
`/og/site.jpg`, JS stamps), `static/i18n/pending/faq.{en,ru,zh,hi,fa}.json` (153 keys each, same set),
`static/images/faq/` (12 files), `static/images/og/faq.jpg` (1200x630, 84 KB).

## What changed
- 12 sections (was 8), 51 questions (was 27), answers 2-4 sentences. New sections: Upload and speed, Model categories,
  Cars and vehicles, Languages and reports. Every section keeps its anchor id; old question ids unchanged (`#viewer-open`...).
- Rewritten to the V3 facts: what happens after upload (black scene, stages, then scene), camera (right = pan, middle = orbit),
  presets and hotkey badges, scenes, rig how (fast rig, category template, background QA), joint fix (Blueprint `B`, symmetry,
  re-rig after 15 s or by button), animations (per-category library, generated reference videos, custom clips), textures
  (own kept, auto PBR), exports (GLB, FBX, USDZ, ZIP, Unity package, Blender; OBJ kept from live), languages, shared agent.
- New: skeleton confirmation is offered, never required; categories (all ten, equine/bird/hands/pose answers, reference-leg
  rule, file's own rest pose); vehicles (segmentation, Parts panel accept/redraw/order, wheels to cylinder colliders, FFD
  symmetry, hinged doors, interior and IK driver, paid clouds only by button); rig quality (regression tests, release gate);
  defect buttons in the Telegram notice.
- Design: same look; chips and section icons for the 4 new sections (animated like the old ones); category strip (6 cards),
  scenes strip (4), two vehicle figures; page-local CSS only. Checked at 1280 px and in a 375 px frame: no horizontal scroll.
- The pricing/credit facts: the live FAQ has none, so nothing was kept or added. Links to `/buy-credits` in the footer row.

## i18n
- Pending keys: all `faq_*` used on the page (reused keys keep their en/ru text from the shared JSON; zh/hi/fa did not exist
  for any `faq_` key, so they are all new), plus `seo_faq_title`, `seo_faq_description` (the names the backend reads).
- Changed text under an existing key (merge overwrites): faq_title, faq_lead, faq_a_viewer_open, faq_a_viewer_camera,
  faq_a_viewer_tools, faq_a_rig_how, faq_q_rig_fix, faq_a_rig_fix, faq_a_rig_models, faq_a_anim_which, faq_a_gen_image,
  faq_a_ch_kept, faq_a_export_formats, faq_a_export_engines.
- Dropped keys: none. Shared keys used: footer_*, nav_* (present in all five).
- Picture alt texts are `faq_alt_*`; the server does not translate `alt`, so a small inline script sets them from I18n.

## Images (all real, none generated)
Gallery posters (task ids): wolf 8486c5fe (quadruped card), og image also uses f25a3a94 (biped), 7d55f8e9 (horse), 4a6acf49
(chicken). V3 pool (`images/v3/README.md`): rider-weights (biped card), horse-028d1acd stand gif frame (equine), bird-weights
(bird), hand-stand gif frame (hands), car-motorhome-input (vehicle card and figure), car-motorhome-shell, car-motorhome-isolated-sheet
(run c91d4e7f). Scenes: cathedral, sponza, river-glade, cfa2e6a1-jungle-ruins review renders. All <= 70 KB, WebP/JPEG,
width/height set, lazy.

## SEO
Title (55): "AutoRig FAQ: Auto-Rigging for Characters, Animals, Cars"; description (151); keywords; canonical `/faq`; og:*, twitter:*
(image `/static/images/og/faq.jpg`, 1200x630); hreflang en/ru/zh/hi/fa/x-default; JSON-LD WebPage + FAQPage (51 Q/A, same
text as the page) + BreadcrumbList; one h1; 12 internal links in the footer row plus in-answer links (/, /how-it-works,
/animal-rig, /t-pose-rig, /gallery, /blender-plugin, /developers).

## Checks (scratchpad `check.py`, `check2.py`, `check3.py`)
- HTMLParser: unclosed tags [], mismatches [], h1 = 1, duplicate ids [], 12 images all with alt/width/height.
- Local /static targets: none missing; 20 remote targets (`curl`): all 200.
- JSON-LD parses (3 nodes, 51 questions). Pending JSON: 5 files, equal key sets (153); every `data-i18n*` key exists in all five.
- Leftover scan: no Cyrillic in zh/hi/fa; only technical terms in Latin (GLB, FBX, Unity, Blender, Sponza, Outliner...).
- Server `translate_html` (backend `user_language`) run on the page for ru/zh/hi/fa with pending merged: translates h1 and questions.
- Headless Chrome render of the page with live CSS: desktop and a 375 px iframe, readable, no horizontal scroll. No JS file touched.

## Open questions / for the lead
1. `/faq` is neither in `FULL_PAGE_PATHS` nor `ADVERTISED_PATHS` (backend `user_language.py`, `static/js/i18n.js`): the page is
   chrome-scope, `/ru/faq` etc. are noindex with canonical to English, and my hreflang links will be dropped/ignored. To make
   the five versions real add `"/faq": "faq"` to `ADVERTISED_PATHS` (SEO keys `seo_faq_*` are in pending) and `/faq` to
   `FULL_PAGE_PATHS`. Until then an inline script sets `dir=rtl` on `<main>` for Persian.
2. JSON-LD is English only (server skips script blocks); fine while `inLanguage` is `en`.
3. Camera: spec says right = pan, middle = orbit; the live text said left = orbit. I wrote both (left-drag also orbits).
   Please confirm. Also «joint editor» in the icon list became «Blueprint»; confirm the icon name.
4. OBJ (static copies) is kept from the live answer although spec §1 does not list it; remove if wrong.
5. Not stated, so not written: pricing, the top-5 similar models (operators' chats only), the missing-textures prompt at
   intake. Video as an input (live answer) kept although §1 lists only model/picture/car/text.
6. Wanted from the lead later: a Parts-panel and a Blueprint viewer screenshot for the vehicle and rig answers.
