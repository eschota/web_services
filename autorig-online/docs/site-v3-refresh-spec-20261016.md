# Site refresh to V3 — spec for the page agents (2026-10-16)

Owner order (2026-10-16): every page of autorig.online is brought to the current V3 product: text, design,
screenshots, SEO metadata (for search engines) and localization, including cars, animals, hands and wings.
Five agents work in parallel, one page each; the lead reviews each result, fixes it, deploys it and hands out the
next page, until the whole site is done: pages, headers, footers. Layout and localization are then checked separately.

Read this whole file before touching anything. The lead (Fable) deploys and commits; you do not.

## 1. What V3 is (the facts the text must tell)

Write from these facts. Do not invent numbers, prices or features. Where the old page states a price or a credit
amount, keep it unless this file says otherwise.

* **One conveyor for any input.** A task starts from a 3D model (GLB, FBX, OBJ, ZIP with textures), a picture (image →
  3D model → rig), a picture of a car (the vehicle conveyor), or text. One upload zone on the home page takes all of
  them («Drop a 3D model, a picture, a picture of a car, or anything»). 1 task = 1 viewer: every task page is the
  Unity WebGL viewer with the agent chat.
* **Rig in seconds, animated at once.** The fast rig builds the skeleton and skin from the geometry in seconds, the
  animation clips are retargeted at once, and the quality checks (joint placement, skin tears, limb collisions,
  numeric deformation QA) run in the background of the viewer. The target is a rigged, animated model within one
  minute from pickup. The skeleton confirmation is *offered* in the viewer, never required: the animation is made
  without waiting for it.
* **Categories, each with its own skeleton template, fit rules, skin rules, clip library and QA** (one handoff per
  category in the engine): biped / humanoid (incl. a reference-leg rule for asymmetric poses), quadruped (dogs, cats,
  bears, cows, pigs…), equine (horses, deer, zebras: scapula, elbow, mid-neck, leg profile joints; planted-hoof gaits
  walk / trot / gallop), monster, serpent, bird (wings), multilimb, hands (a single hand, gloves, FPS arms: per-finger
  joints and hand clips), primate, vehicle (cars). Birds: wings are rigged as wings, never as arms.
* **Vehicles (cars) are rigged too.** From one picture: segmentation of body, wheels, doors, hood, trunk, hatches,
  glass, lights, bumpers (GroundingDINO + SAM2 masks, Qwen-image edits), every part isolated and restored as a whole
  object, the bare shell drawn without openables and glass, each part modelled as its own 3D object, wheels
  normalised to cylinder colliders, body symmetry corrected by FFD, doors hinged, interior with seat, wheel, pedals
  and gear stick, a rigged driver seated by IK. The parts are reviewed in the viewer («Parts» panel: accept / redraw
  / order as 3D). Paid 3D clouds (Tripo, Hunyuan cloud) only on the person's explicit button; the free farm is the
  default.
* **Textures.** A model's own textures are never lost. A model without textures gets PBR textures automatically
  (albedo, roughness, metallic, normal, emissive) from its own projections; the textured model is what the vision
  checks and the previews use.
* **Animation library per category.** Clips come from generated reference videos per category (walk, trot, canter,
  gallop, idle, rear, look around, attack…), extracted once, retargeted automatically onto every new rig with numeric
  QA per clip. Procedural clips are the fallback. Custom clips can be added in the viewer (the clip set).
* **The viewer.** Unity WebGL. Opens on a black scene at once, plays the stages (voxels, erosion, bones, rig) while
  the mesh loads, then loads the chosen environment scene (baked lighting, reflection and light probes, colliders;
  scenes such as cathedral, Sponza, river glade, jungle ruins). Blueprint mode (B) to edit bones with symmetry; re-rig
  15 s after the last edit or by the button; camera presets for face, elbows, hands, knees, feet, heels; hotkey
  badges on every control; right mouse = pan, middle mouse = orbit; a session agent chat per task (one shared agent
  for everyone who opens the task, in the task creator's language); defect buttons and the top-5 similar models in
  the Telegram notice; downloads: GLB, FBX, USDZ, ZIP, Unity package, Blender.
* **Quality gates.** Every bad model becomes a regression test; every engine release passes the autotests before it
  goes live; rig quality comes before speed.
* **Languages:** en (no prefix), ru, zh, hi, fa (right-to-left) — `/ru/…`, `/zh/…`, `/hi/…`, `/fa/…` for the main
  pages; the SEO articles have `-ru`, `-zh`, `-hi` copies.
* **Do not say:** «V2», «animal rigging V2», OpenPose body marker detection, Mixamo-style classic converter pipeline,
  «T-pose preparation step», «Blender 4.x only», Unity version numbers, anything «coming soon» that you cannot
  verify. Replace such passages with the V3 facts above. The Blender plugin and the API still exist: describe them as
  they are on their pages, do not promise new plugin features.

## 2. The site

* Source: `R:\autorig\autorig-online\static` (branch `codex/repair-rig-transport-20261006`, local checkout).
* A page is one HTML file; the header and footer are injected by the backend from `static/partials/site-header.html`
  and `site-footer.html` (do not copy them into pages; do not edit them unless your assignment says so).
* Canonical paths: `backend/main.py`, `STATIC_PAGE_CANONICAL_PATHS` (about line 16270).
* i18n: `static/i18n/{en,ru,zh,hi,fa}.json`; markup uses `data-i18n="key"`, `data-i18n-placeholder`,
  `data-i18n-title`, `data-i18n-aria-label`; `I18n.t()` in JS. **Do not edit the shared JSON files directly** (five
  agents at once). Put your keys in `static/i18n/pending/<page>.<lang>.json` (flat `{key: text}`, UTF-8, one file per
  language, all five languages, same key set). The lead merges them with `tools/i18n_merge.py`. Prefix every new key
  with the page slug (`home_`, `hiw_`, `animal_`, `guides_`, `faq_`…). Keep existing keys you still use; list the keys
  you dropped in your report so the lead removes them.
* Images: `static/images/<page>/…`. Only real V3 media. Sources:
  * gallery posters: `https://autorig.online/api/gallery?per_page=40&sort=date` → `thumbnail_url` (`/thumb/<task>`)
    and `video_url`; `&category=vehicle|quadruped|biped|hands|bird` filters where it works (check the JSON);
  * environment scene renders: `https://autorig.online/api/mt/unity/scene-media/<river-glade|cathedral|sponza|cfa2e6a1-jungle-ruins>/review-1280x720.jpg`;
  * the lead supplies viewer screenshots (blueprint side view, the parts panel of a car, the hand rig, a bird) into
    `static/images/v3/` — ask through your report if you need one that is not there yet;
  * never use the in-app browser for screenshots (it is one shared pane), never download from other sites, never
    use stock art or AI-generated pictures.
  * Sizes: hero ≤ 1600 px wide, cards ≤ 800 px, JPEG quality 82 or WebP; every image ≤ 300 KB; `width`/`height`
    attributes and a real `alt`; `loading="lazy"` below the fold. Use Pillow (`py -3.11 -c "import PIL"`).
* Styles: the site CSS is in `static/css/` (shared; do not edit shared CSS — page-local styles go into a `<style>`
  block at the top of your page or a `static/css/<page>.css` file you own).
* Deploy is the lead's: `sudo python3 /srv/autorig/tools/live_static.py put <rel> <file>` on the VPS. You never run
  it, never ssh, never git commit.

## 3. What each page agent delivers

1. **Text** to the V3 facts: hero, sections, FAQ answers, CTAs, buttons. Short sentences, concrete, no marketing
   fluff. English is the source; the other four languages are translations of the same content (native wording, not
   literal; fa right-to-left).
2. **Design**: keep the site's look (fonts, colours, header/footer); fix what is dated (V2 badges, old flow
   diagrams, broken grids); mobile layout at 375 px with no horizontal scroll; dark/light theme both readable.
3. **Screenshots / media** as in §2, with captions in i18n.
4. **SEO**: `<title>` (≤ 60 chars, with «AutoRig» and the page's primary keyword), `<meta name="description">`
   (≤ 160 chars), keywords, canonical, `og:*` and `twitter:*` (image 1200×630 under `static/images/og/`),
   `hreflang` links for en/ru/zh/hi/fa where the page has language versions, JSON-LD (WebPage / FAQPage / HowTo /
   SoftwareApplication as fits; valid JSON), one `<h1>`, a sensible `h2/h3` outline; internal links to the related
   pages (categories, guides, viewer, API, Blender plugin) — the crawl-critical links must be in the HTML itself,
   never only in JS.
5. **Localization**: every visible string goes through i18n keys; the `-ru/-zh/-hi` copies of an article (where
   they exist) carry the same new content in their language with their own `<title>`/description/`lang`/hreflang.
6. **Checks you run yourself before reporting** (put the commands and results in the report):
   * `py -3.11 -c "import html.parser,sys; ..."` or `tidy -q -e file.html` (if available): no unclosed tags; exactly
     one `<h1>`; every `<img>` has alt, width, height; every local `src`/`href` target exists on disk or answers 200
     on `https://autorig.online` (`curl -sI`);
   * every `data-i18n` key on the page exists in all five pending files (or already in the five shared JSON files);
     no untranslated Russian/English leftovers in the other languages;
   * JSON-LD blocks parse (`python -c "import json; json.loads(...)"`);
   * `node --check` for any JS you touched;
   * a quick render check of the page's own structure: `curl -s https://autorig.online/<path>` of the live page vs
     your file to make sure you started from the live content (the local checkout can be behind production:
     **always diff your page against the live HTML first** and start from the live version).
7. **Report** `docs/site-v3-refresh/<page>.md`: what changed (sections), new/dropped i18n keys, images added (source
   task ids / scene ids), SEO fields, the checks with their output, open questions for the lead. Keep it under 80
   lines.

## 4. Round 1 assignments

| agent | page file(s) | path | notes |
|---|---|---|---|
| A | `index.html` | `/` | home: hero, the single upload zone copy, pipeline section (replace «Animal V2» tags), features, FAQ JSON-LD; `/ru/ /zh/ /hi/ /fa/` use the same file through i18n |
| B | `how-it-works.html` | `/how-it-works` | the whole flow is V3: rewrite sections (OpenPose/T-pose/HDRP passages out), HowTo JSON-LD, stage screenshots |
| C | `animal-rig.html` + the animal pages' shared copy | `/animal-rig`, `/dog-auto-rig` … | quadruped / equine / bird / serpent / monster categories, gaits, the horse template; this page has no i18n keys yet — add them |
| D | `guides.html` + `t-pose-rig.html` | `/guides`, `/t-pose-rig` | guides index: add cards for vehicles, hands, birds, equines; the T-pose article becomes «any pose, the reference-leg rule, bind pose from the file» |
| E | `faq.html` | `/faq` | V3 viewer, agent, categories, vehicles, textures, exports; FAQPage JSON-LD; keep answers short |

Round 2 (after review): `developers.html`, `blender-plugin.html`, `buy-credits.html`, `gallery.html`, the format
articles (`glb-auto-rig`, `fbx-auto-rig`, `obj-auto-rig`, `auto-rig-obj*`, `glb-vs-fbx*`), `mixamo-alternative*`,
`rig-glb-unity*`, `rig-fbx-unreal*`, `animation-retargeting*`, `face-rig-animation*`, `image-to-rigged-3d-character*`,
`t-pose-vs-a-pose*`, `terms-of-use`, `user-agreement`, then the header and footer partials, then a layout pass and
a localization pass by two separate agents.

## 5. Working rules

* Start from the live HTML (`curl -s https://autorig.online/<path> > /tmp/live.html`) and diff it with your file; the
  live page is the truth.
* Touch only your files (your page, its language copies, `static/i18n/pending/<page>.*.json`,
  `static/images/<page>/`, `docs/site-v3-refresh/<page>.md`). Never the shared JSON, CSS, partials, backend, other
  pages.
* No git operations, no deploys, no ssh, no browser pane, no paid APIs, no external downloads besides
  autorig.online.
* If a fact you need is not in §1, write the sentence without it and ask in the report; do not guess.
* Finish the whole page; a half-done page is not a result.
