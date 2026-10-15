# /animal-rig — site V3 refresh, page agent C (2026-10-16)
Files: `static/animal-rig.html`, `static/images/animal-rig/` (new, 19 images), `static/images/og/animal-rig.jpg`,
`static/i18n/pending/animal.{en,ru,zh,hi,fa}.json` (167 keys each). The old V2 videos in `static/videos/animal-rig/`
are untouched: the per-animal pages still use their posters (see "Per-animal pages").

## Started from live
`curl https://autorig.online/animal-rig` vs the file: identical except backend injections (`data-i18n-scope`,
`__AUTORIG_LANG__`, og:locale/og:image:alt, server header, stamped CSS `?v=`). Live == local content.

## What changed
Whole page rewritten, same look (dark hero, cards, yellow CTA), V2 text/videos/before-after/VideoObject JSON-LD removed.
1. Hero: V3 kicker, h1 «AI animal rigging for any four-legged, winged or many-legged model», lead, 2 CTAs, category pills,
   real V3 check sheet (white horse gallop).
2. Six categories (quadruped, equine, bird, serpent, monster, multilimb): real gallery posters, animals, one-line template.
   Serpent has an inline SVG chain-of-joints diagram (no serpent poster in the gallery).
3. Four-step flow (drop anything, category + skeleton, rig + clips in seconds, background checks; textures sentence).
4. Equine template: scapula, elbow, mid-neck, leg profile, planted-hoof gaits (walk/trot/gallop), check sheet + 2 gifs.
5. Birds: wings are wings; rig sheet, skin weights with skeleton, gif.
6. Clip library from generated reference videos (3 cards + custom clips in the viewer).
7. Skeleton offered, never blocking (B blueprint mode, re-rig 15 s, camera presets).
8. Unity viewer: black scene first, stages, environment scenes (2 renders), shared session agent, hotkeys, downloads.
9. Quality gates (numeric QA per clip, background checks, bad model = test, autotests before release).
10. Gallery strip: 5 real public tasks linked to `/task?id=`; 12 per-animal page links (in the HTML, SEO); FAQ (6, visible +
    FAQPage JSON-LD); "Keep exploring" links (/guides, /how-it-works, /gallery, /t-pose-rig, /blender-plugin, /developers,
    /faq); CTA band. Light theme tokens added (`[data-theme="light"] .animal-page`), logical CSS (RTL-safe).

## SEO
title (52): «AI Animal Rigging Online: Horse, Dog, Bird | AutoRig»; description (150); keywords; canonical
`https://autorig.online/animal-rig`; og:* + twitter:* with `static/images/og/animal-rig.jpg` 1200x630 (4 real posters);
hreflang en/ru/zh/hi/fa/x-default (see open question 1); one h1; h2/h3 outline; JSON-LD WebPage + BreadcrumbList + FAQPage.

## i18n
New keys: all 167 are `animal_*` plus `seo_animal_title` / `seo_animal_description` (the backend reads `seo_<slug>_*` for
`SEO_KEYS`). The page had no keys before; none dropped. `alt` has no server/`I18n` support (`_ATTR_KEYS` knows only
placeholder/title/aria-label), so images carry `data-i18n-alt` and a 15-line script at the page end applies it after
`I18n.init()` and on `languageChanged`. JSON-LD stays English.

## Images (all real V3, resized, every file <= 100 KB)
- Gallery posters (`/thumb/<task>` 1080x1920, cropped): wolf 8486c5fe, horse c45fcde9, green bird 395b0c1d, ice monster
  23bb65c7, spider 6a490b56, bison c8e0491f, bear decc58d1, elephant 7deaaa48, zebra e063c134, sheep 43dde6df.
- From `static/images/v3/`: horse sheets/gifs runs 028d1acd + 75550318, bird sheet/weights/gif run 48bc7952 (task 395b0c1d),
  scenes river-glade and jungle-ruins (cfa2e6a1).
- og image: 2x2 of wolf/horse/bird/monster posters + text, made with Pillow.
- Gallery API has no V3 category: `rig_type=` is V2-only (`dog` 1, `cat` 2, rest 0), `category=` is ignored. I picked the
  posters by looking at the newest 400 gallery thumbnails.

## Checks (scratchpad `animal_c/check.py`, `build.py`; headless Chrome on a local copy)
- HTML parser: no unclosed/mismatched tags, 1 `<h1>`, 19 `<img>` all with alt/width/height, no duplicate ids -> OK.
- All local src/href exist; all 25 internal links answer 200 on autorig.online (incl. 12 per-animal pages, 5 task ids).
- i18n: five pending files have the same 167 keys; every `data-i18n*` key on the page exists; no pending key unused;
  no Cyrillic in zh/hi/fa; every ru value has Cyrillic; no value equals English except hi `animal_more_faq` («FAQ»).
- JSON-LD: 3 blocks parse (`json.loads`). No JS besides the alt script (inline, simple).
- Render (headless Chrome, local copy, no pane): desktop 1280 OK; at 375 and 320 px `scrollWidth < innerWidth` (no
  horizontal scroll); cards stack to 1 column. Dark theme looked at; light theme not rendered (tokens only).

## Per-animal pages (/dog-auto-rig, /horse-auto-rig …) — NOT edited, where the text lives
All generated in `backend/main.py` ~11785-12400: `GALLERY_RIG_TYPES`, `RIG_ARTICLE_LANGS` (UI strings 4 langs, only `en`
indexed via `RIG_ARTICLE_INDEXED_LANGS`), `RIG_ARTICLE_TYPES` (label/shape/use/tip per animal, 12 animals),
`_rig_article_text` (title, description, h1, lead, workflow, FAQ of 3 questions), `_build_rig_article_html`
(template, JSON-LD TechArticle/FAQPage/Breadcrumb/ItemList, the hard-coded «Open V2 animal rig examples» at ~12256),
`RIG_ARTICLE_STATIC_EXAMPLES` + `DEFAULT_RIG_ARTICLE_STATIC_EXAMPLE` (V2 titles «Cat V2 animal rig…», V2 posters from
`/static/videos/animal-rig/*-v2-rig-poster-*.png`, links `/animal-rig#examples` and `#presentation`), `_rig_article_examples`
(examples = old tasks with `input_type == animal`; V3 tasks all report `humanoid`, so pages show 0-2 old tasks or the V2 poster).
Should change: (a) drop «V2» in the static example titles and the «Open V2…» string; (b) point the static posters to
`/static/images/animal-rig/cat-*.jpg` and `#presentation` -> `#examples` (that anchor no longer exists); (c) mention V3
(skeleton + clips in seconds, category template) in lead/workflow/FAQ; (d) deer -> equine template, horse/deer tips with
scapula/elbow/mid-neck and planted-hoof gaits; (e) examples must come from the V3 category once the gallery exposes it;
(f) add `/zebra-auto-rig`, `/bird-auto-rig` etc. only if the lead wants them (no pages exist today).

## Open questions for the lead
1. `/animal-rig` is in neither `FULL_PAGE_PATHS` (`backend/user_language.py` L57 and `static/js/i18n.js` L35) nor
   `ADVERTISED_PATHS` (value `"animal"`, so `seo_animal_*` and the hreflang cluster work). Until you add it, `/ru/animal-rig`
   is chrome-scope (no RTL for fa, `noindex`, canonical to en) and my static hreflang points to those URLs; the backend
   replaces my hreflang once the page is advertised. Please add it in the same deploy as the keys.
2. Merge: `py -3.11 tools/i18n_merge.py --page animal --write`.
3. `translate_html` ignores `data-i18n-alt`; adding it to `_ATTR_KEYS` would translate the alt server-side.
4. Gallery: a V3 category field in `/api/gallery` items (and `?category=`) would let this page and the per-animal pages
   pull live examples. No serpent/monster/multilimb poster pairs beyond the three used; a serpent viewer shot would help.
5. Facts I stayed careful with: serpent/monster/multilimb card texts are definitional (no feature claims); only equine gaits
   are named (walk/trot/gallop) as in §1; no bird clip names; the claim «category is detected by V3» is not tied to geometry.
