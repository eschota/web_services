# /gallery — site V3 refresh, page agent C (2026-10-16)

Files: `static/gallery.html`, `static/images/og/gallery.jpg` (1200x630), `static/i18n/pending/gallery.{en,ru,zh,hi,fa}.json`
(33 keys each). Started from live: live == local except backend injections (header/footer, hreflang, canonical, lang boot,
server-rendered cards, seo_social tags, stamped `?v=`).

## What changed (and what did not)
- Untouched, verified by diff: the whole `<script>` block and script includes (byte-identical), every id (`gallery-total`,
  `gallery-rig-type`, `gallery-grid`, `gallery-prev`, `gallery-page`, `gallery-next`), every `<option value>`, the grid
  placeholder `<div id="gallery-grid">` + `<div class="card">Loading…</div>` that `gallery_page()` replaces with the
  server-rendered cards (the regex from main.py still matches my file: checked), the GA snippet.
- Texts: h1 «Gallery of rigged 3D models», intro (V3: characters, animals, hands and cars; newest first; open one in the
  viewer), empty state «No rigged models of this type yet. Try another rig type.» (key `gallery_empty`, used by the script).
- New section «About these results» below the grid (3 short paragraphs + 5 crawlable internal links: `/`, `/animal-rig`,
  `/how-it-works`, `/guides`, `/faq`), page-local `<style>`, dark/light via the site tokens, no overflow.
- SEO: title (47) «Rigged 3D Model Gallery: Real Results | AutoRig»; description (151); keywords; canonical; og:* and
  twitter:* with `static/images/og/gallery.jpg` (8 real posters: horse, car, bird, hand, wolf, character, monster, spider);
  JSON-LD CollectionPage + BreadcrumbList; one h1. No hreflang in the file (the backend injects it).
- Image sources (task ids): horse c45fcde9, car b576368c, bird 395b0c1d, hand c5c53dc6, wolf 8486c5fe, character 9838cdd2,
  monster 23bb65c7, spider 6a490b56.

## i18n
- The 20 page keys keep their names. Pending files carry all 20 + `gallery_empty` + `seo_gallery_title/description` (the
  English ones did not exist; zh/hi had none) + 10 new `gallery_about_*` keys.
- Missing translations filled: Hindi had 15 of the page keys still in English (rig type label, «All rig types», humanoid and
  the 12 animal names, the subtitle): now translated. ru/zh/fa already complete.
- Changed existing texts (merge reports them as overrides): `gallery_page_title`, `gallery_page_subtitle`, `gallery_empty`
  (all five languages), plus the Hindi fixes above.
- Not mine, still English in `hi.json`: `gallery_subtitle`, `gallery_home_stats` (home page, agent A).

## What the filter offers today (V2 presets)
`<select id="gallery-rig-type">`: `all`, `humanoid`, `dog`, `bear`, `cat`, `cow`, `deer`, `elephant`, `giraffe`, `horse`,
`mouse`, `pig`, `rabbit`, `turtle` (same list in the script's `allowed` array and in `GALLERY_RIG_TYPES`,
backend/main.py ~11785). The URL `?rig_type=<value>` (noindex query variant) is passed to `/api/gallery?rig_type=`.

Behaviour found in the backend (`api_get_gallery` ~8596, `_gallery_rig_icon_key` ~11802):
- The key comes from `viewer_settings.rig_v2_animal_detection` and only when `input_type == "animal"` (the V2 animal path).
  Every V3 task reports `rig_icon_key: "humanoid"` (the newest 200 gallery items are all `humanoid`, including wolves,
  horses, birds, cars and hands). Live counts: `rig_type=dog` 1, `cat` 2, `horse/deer/bear` 0.
- A filtered request loads every matching row (no SQL limit) and filters in Python, then slices: slow as V3 grows.
- Icons: `static/Icons_png/<Name>.png` exists only for Human + the 12 animals; `rig-icons.js` falls back to Human.
- `category=` is ignored by the API.

## What V3 categories would need from the backend (not edited)
1. A stored category on the task/run (biped, quadruped, equine, bird, serpent, monster, multilimb, hands, primate,
   vehicle), written when the V3 run finishes, and exposed as a new field on gallery items (e.g. `category`); keep
   `rig_icon_key` for icons until the front end is switched.
2. `GALLERY_RIG_TYPES` (and the `allowed` array in the page script, and `<option>` values) extended or replaced by the
   category slugs; the filter applied in SQL on that column, with `has_more`/`total` computed in SQL.
3. `/gallery?rig_type=` must keep accepting the old animal slugs (old links); map them to category + animal if wanted.
4. Icons for the new categories in `static/Icons_png/` (bird, vehicle, hands, serpent, monster, multilimb) and the
   `RIG_ICON_ANIMAL_KEYS` list in `rig-icons.js`.
5. i18n keys `gallery_rig_type_<slug>` per category (I can write them in five languages once the slugs are fixed).
6. The server-rendered cards (`seo_social.gallery_cards_html`) use alt «Rigged 3D model» and the same text as the time
   label; a category name plus the poster title would be a better alt and anchor text.
7. The per-animal pages and `/animal-rig` could then link to `/gallery?category=…` or list live examples.
Suggestion (not done, the markup is frozen): `<label>` of the filter has no `for="gallery-rig-type"`.

## Checks (scratchpad `gal_c/g_check.py`, `tools/site_page_check.py static/gallery.html --pending gallery`)
- site_page_check: h1 1, canonical set, JSON-LD CollectionPage + BreadcrumbList, missing i18n keys 0 in all 5 languages: OK.
- Tag balance: no unclosed/mismatched tags (original also clean); ids / data-* / value attributes: nothing lost, only the new
  `gallery-about-title` added; script block identical; backend card injection regex matches (1).
- Pending: same 33 keys in 5 files; no Cyrillic in zh/hi/fa; every ru value has Cyrillic; no value equals English except hi
  «FAQ». JSON-LD parses. 5 internal links answer 200. OG image 141 KB, 1200x630.
- Render (headless Chrome, local copy, scripts stripped): hero, filter card and the new section look right; layout is the
  existing card layout, new section stacks on mobile (no fixed widths).
- No JS touched, so no `node --check`.

## Open questions
1. Merge: `py -3.11 tools/i18n_merge.py --page gallery --write`.
2. When `/gallery` category work starts I can add `gallery_rig_type_*` keys and the option list in one pass.
