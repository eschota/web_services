# Home page `/` (static/index.html) - V3 refresh, agent A, 2026-10-16

Started from the live HTML (`curl https://autorig.online/`): it equals the local file except for what the backend injects
(header/footer partials, hreflang, og:locale, `?v=` stamps, the 12 server-rendered gallery cards). Nothing else was behind.

## What changed
* **Hero**: new h1 «Auto rig any 3D model in seconds», subtitle (V3: skeleton+skin, clips at once, viewer with agent), upload zone copy
  «Drop a 3D model, a picture, a picture of a car, or anything» + formats line, the T-pose notice is now «Any pose works…»,
  a row of 6 category chips (anchors to the sections), the busy-overlay tips rewritten (V3 stages, no «5 minutes», no T-pose),
  the Russian leftover in the orientation hint got an i18n key. All ids, data-*, form elements and the gallery placeholder kept.
* **Gallery block**: h3 -> h2, new title/subtitle keys (no stale «7,124+»); JS still overwrites the subtitle with the live stats.
* **Pipeline section** (the «V2 rigging pipeline» kicker and «Animal V2» tag are gone): kicker/h2/lead, 4 steps (drop anything, fast rig,
  animated at once, checked while you look), 3 feature cards (any pose / textures stay / web+plugin+API).
* **New sections**: categories (7 cards: humanoids+primates, quadrupeds, horses/deer/zebras, birds+wings, hands+gloves, cars, monsters/serpents/multi-limb),
  vehicles («Rig a car from one picture»: 6 steps, paid-cloud-only-by-button note, 3 real figures), viewer («One task, one viewer»: black scene first,
  blueprint B, camera presets, shared agent chat, downloads, 4 environment scenes), internal links row (8 pages), visible FAQ (8 Q&A).
* Page-local CSS: one `<style>` block, classes `hv3-*`; logical properties (RTL ok), theme tokens only, breakpoints 960 / 560, 2-column cards at 375 px.

## SEO
* title (56): «AutoRig: AI Auto Rigging for Any 3D Model, Animals, Cars»; description (146); keywords rewritten (no «animal rigging V2»).
* canonical `/`, hreflang en/ru/zh/hi/fa/x-default (the backend rewrites the same set), og:* / twitter:* with `static/images/og/home.jpg` 1200x630
  (composite of real gallery posters on a river-glade scene render). og:video kept as it was.
* JSON-LD: WebPage, SoftwareApplication (V3 featureList, `DesignApplication`, aggregateRating **removed**: 4.8/150 was not verifiable), VideoObject (kept), FAQPage
  (same 8 questions as the visible FAQ). One h1; outline h1 > h2 (gallery, pipeline, categories, vehicles, viewer, FAQ) > h3.

## i18n
* `static/i18n/pending/home.{en,ru,zh,hi,fa}.json`, 96 keys each, same key set. Prefix `home_`; plus `seo_home_title` / `seo_home_description`
  (the backend reads exactly these names for the /ru /zh /hi /fa title and description; they **override** the existing shared values).
* Shared keys reused: `footer_animal_rig`, `footer_how_it_works`, `footer_faq`, `nav_guides`, `nav_gallery`, `nav_blender_plugin`, `nav_api`, `nav_buy`,
  `home_demo_youtube_*`, gallery/history/queue/upload-progress keys, rig-type option keys.
* **Dropped from this page (lead: grep other pages before removing):** `hero_title`, `hero_subtitle`, `upload_title`, `upload_subtitle`, `tpose_notice`,
  `upload_busy_tip_1..4`, `gallery_title`, `gallery_subtitle`, `home_pipeline_kicker`, `home_pipeline_title`, `home_pipeline_lead`,
  `home_pipeline_tag_humanoid`, `home_pipeline_tag_animals`, `home_pipeline_tag_nonhumanoid`, `home_pipeline_tag_blender`, `home_pipeline_feature_1..3_title`, `home_pipeline_feature_1..3_desc`.

## Images (`static/images/home/`, all <= 300 KB, width/height set, lazy below the fold)
* from `static/images/v3/`: `cat-equine.gif` (horse run 028d1acd, gallop), `cat-bird.gif` (run 48bc7952, task 395b0c1d), `cat-hands.gif` (run d3ef5ccb, task 96f98e36),
  `cat-vehicle.jpg`, `vehicle-input.jpg`, `vehicle-parts-map.jpg`, `vehicle-isolated-sheet.jpg` (run c91d4e7f, task c651e1f4, rv_motorhome),
  `scene-cathedral|sponza|river-glade|jungle-ruins.jpg` (unity scene-media review renders, 800x450).
* gallery posters (`/thumb/<task>`): `cat-biped.jpg` 12dbfc31, `cat-quadruped.jpg` 50a9139d (wolf), `cat-monster.jpg` 23bb65c7. OG: wolf 50a9139d, horse 7d55f8e9, rider 12dbfc31.
* The gifs were re-encoded (palette, same frames except hands: all 29) to get under 300 KB.

## Checks (scratchpad `check_index.py`, run on the final file)
* html.parser stack: no unbalanced tags; h1 count 1; every `<img>` has alt, width, height; no duplicate ids; every original id and data-* attribute kept;
  anchors that `app.js` and the gallery injector need (`#convert-form`, `#upload-zone`, `#file-input`, `#start-btn`, gallery placeholder card, `#history-list`, `#queue-active`) present.
* local `/static/...` targets exist on disk; 8 internal links answer 200 on autorig.online; 6 in-page anchors resolve.
* every `data-i18n*` key exists in the 5 pending files or the 5 shared JSON files (140 keys); pending key sets identical; no value equal to English in ru/zh/hi/fa;
  each language only uses its own script; `{placeholders}` equal; title <= 60 and description <= 160 in all 5 languages.
* 4 JSON-LD blocks parse; FAQPage equals the visible FAQ. No JS touched (no `node --check` needed).
* `user_language.translate_html` run on the file with merged ru/zh/hi/fa keys: new sections translate server side.
* Not done: no visual render (no browser/local server allowed); please look at 375 px, dark and light, and fa (rtl).

## Open questions for the lead
1. `alt` is not localized by the site (`i18n.js` / `translate_html` only do placeholder, title, aria-label); alts are English. Add `data-i18n-alt` support if wanted.
2. Price conflict: the HTML said «3 credits» for picture -> character, shared `hero_subtitle` said «Unlimited Monthly». The new hero states no price; the hidden image-panel text (`image_gen_subtitle`) is untouched.
3. The home demo video (`rigging-demo-*`, VideoObject, og:video, dated 2026-05) may predate V3; a V3 clip would be better.
4. Missing media: viewer screenshots (blueprint side view, Parts panel, agent chat) - the viewer section uses scene renders only.
5. The gallery rig-type filter lists only the old animal presets (`GALLERY_RIG_TYPES`), no vehicle/bird/hands; the server-rendered gallery cards have the alt «Rigged 3D character» (seo_social).
6. Does the Unity WebGL name, «re-rig 15 s after the last edit» and «top-5 similar» wording fit the public site? I kept the first two (spec §1), omitted the Telegram notice features.
