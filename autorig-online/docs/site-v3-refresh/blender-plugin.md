# /blender-plugin (static/blender-plugin.html) - V3 refresh, agent A, round 2, 2026-10-16

Started from the live HTML (`curl https://autorig.online/blender-plugin`): equals the local file except the injected header/footer,
`__AUTORIG_LANG__`, og:locale. The page had no i18n keys.

## What changed
* **Text**: «V2 rigging workflow», «V2 presentation», «V2 preset direction», «1-minute V2 demo» are gone. The plugin is described as the
  Blender-side client of the AutoRig conveyor (wording from the coordinator's brief, not verifiable from plugin code: there is none in the repo).
* **Kept exactly (live facts)**: Gumroad checkout links (`/blender-plugin/checkout?source=...`, class `plugin-buy-link`), the assigned test price texts
  («Assigned test price loads at checkout.», «Your assigned test price: {price}», «Buy Blender Plugin {price}»), «13 unique presets»,
  the real plugin UI screenshot + its caption, the 4 features, the 5 Blender-first steps, the YouTube Shorts and Product Hunt links.
* **Removed**: «Presentation frames from the V2 rigging demo» (10 frames with OpenPose-era labels). Files stay on disk, now unused:
  `static/images/plugin/plugin-short-frames-20260516/` (lead may delete).
* **Added**: section «One conveyor behind the plugin and the website» (V3 facts from spec §1, links to How it works, upload page, guides, gallery, API, FAQ, buy credits),
  «Results of the conveyor» (5 real V3 pictures), step 3 mentions the conveyor, a 4-question visible FAQ.
* **Price script rewritten** (inline, page-owned): it used `link.textContent.includes('Blender Plugin')`, which breaks under translation and is wiped by
  `applyTranslations`. Now the hero button is marked `data-plugin-price`, and the price texts are rendered through `I18n.t` after the translations load (`I18n._initPromise`),
  after every `languageChanged`, and after the offer fetch; English fallbacks inline. `node --check` OK.
* Layout: existing look kept; new `plugin-result-*`, `plugin-faq` styles appended; an `[dir=rtl]` override mirrors the hero gradient; mobile 1 column.

## SEO
* title (51): «AutoRig Blender Plugin: Auto Rig Add-on for Blender»; description (141); keywords; canonical; robots; og:* / twitter:* with
  `static/images/og/blender-plugin.jpg` 1200x630 (the real plugin screenshot + headline).
* JSON-LD: SoftwareApplication (existing fields kept: downloadUrl, Offer USD InStock without a price; V3 featureList, inLanguage) and FAQPage (= the visible FAQ).
  One h1; outline h1 > h2 > h3. Crawl links to the site pages are plain `<a>` in the HTML.
* **hreflang is NOT in the file on purpose**: `/blender-plugin` is not in `ADVERTISED_PATHS` / `FULL_PAGE_PATHS` (`backend/user_language.py`, and `FULL_PAGE_PATHS` in `static/js/i18n.js`).
  Today `/ru/blender-plugin` gets canonical = English + noindex, so a hreflang to it would point at a noindex page. To enable: add `"/blender-plugin": "plugin"` to `ADVERTISED_PATHS`
  and `/blender-plugin` to both `FULL_PAGE_PATHS`; the server then injects hreflang/canonical per language and reads `seo_plugin_title` / `seo_plugin_description` (provided).

## i18n
* `static/i18n/pending/plugin.{en,ru,zh,hi,fa}.json`: 70 keys each, same set, prefix `plugin_` (+ `seo_plugin_title`, `seo_plugin_description`).
* Shared keys reused: `footer_how_it_works`, `footer_faq`, `nav_guides`, `nav_gallery`, `nav_api`, `nav_buy`. Dropped: none (the page had no keys).
* `plugin_feat_4_title` («Blender / Unity / Unreal») is identical in all languages by design.

## Images (`static/images/plugin/`, all <= 300 KB, width/height set)
* kept: `blender-plugin-screenshot-20260516.webp` (real plugin UI, shows the panel in Blender 5.1).
* new, from `static/images/v3/`: `v3-rider-weights.jpg` (run ede89a95, task b13b5a46), `v3-bird-weights.jpg` (run 48bc7952, task 395b0c1d),
  `v3-horse-gallop.gif` (run 028d1acd), `v3-bird-wings.gif` (run 48bc7952), `v3-hand-fingers.gif` (run d3ef5ccb, task 96f98e36).

## Checks (scratchpad `check_plugin.py`)
* html.parser: balanced tags, one h1, every `<img>` has alt/width/height (the Blender logo icon keeps `alt=""`, decorative), no duplicate ids; ids lost vs live: only `plugin-presentation-title` (section removed on purpose).
* Local files exist; internal links `/`, `/buy-credits`, `/developers`, `/faq`, `/gallery`, `/guides`, `/how-it-works` answer 200; the checkout links answer 303 (sign-in redirect, as before).
* 72 i18n keys on the page resolve in all five languages; key sets identical; no untranslated values; own scripts only; titles <= 60, descriptions <= 160 in all languages.
* 2 JSON-LD blocks parse, FAQPage == visible FAQ. `user_language.translate_html` with merged keys translates the page for ru/fa/zh/hi.
* Not done: no visual render (no browser); please look at 375 px, light/dark and fa.

## Open questions
1. Enable `/blender-plugin` as an advertised, full-page path (see SEO) so the five language URLs and hreflang exist.
2. The Shorts video behind «Watch presentation» is the V2-era demo; label is neutral now, a V3 clip would be better.
3. Plugin facts beyond the live page (conveyor client, «result stays in your Blender project») come from the coordinator's brief; confirm with the plugin author.
4. A V3 viewer screenshot or a Blender screenshot of a V3 result would strengthen the page.
