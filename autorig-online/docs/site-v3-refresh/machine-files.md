# Machine-readable and crawl files — site V3 refresh, page agent C (2026-10-16)

## 1. skill.md, llm.txt, llms.txt (all plain files, no backend patch needed)
Sources: `R:\autorig\autorig-online\skill.md` (served by `serve_skill_md`, first candidate `autorig-online/skill.md`),
`static/llm.txt` and `static/llms.txt` (`serve_llm_txt` / `serve_llms_txt`). Live content == repo content before my edit,
so I edited the files (CRLF kept for skill.md, LF for the txt files); no `tools/patch_site_textfiles_v3.py` was needed.
- `skill.md` (6.3 -> 11.4 KB): new sections «What V3 Does» (any input, rig in seconds, categories incl. equine / bird /
  hands / vehicle, vehicles, textures, clip library, skeleton confirmation never required, viewer, downloads, quality
  gates, languages) and a fuller «Public Pages» list; «Agent API» now states what /developers documents: register
  (`POST /api/agents/register`, 15/hour per IP, returns `api_key` + `agent_id`), `Authorization: Bearer` or `X-API-Key`,
  `GET /api/agents/me`, `POST /api/task/create`, `GET /api/task/{id}`, `cached-files`, `prepared.glb`, `animations.glb`,
  `animations.fbx`, `model.glb`, `bundle.zip`, result page `/task?id=`. «Supported Workflows» and «Discovery» rewritten
  (adds `/sitemap/pages.xml`). Removed: the «V2» animal wording.
- Kept word for word (internal contracts I cannot verify as changed): «3D Viewer Settings», «3D Viewer Runtime Contract»,
  «Task Download Bundles», «Backend Worker Routing». They mention the classic converter ZIP path
  `/converter/glb/<guid>.zip`, worker `/server-status`, `animations.glb` GLB-first preview: lead, please confirm they
  still describe the V3 task page, or tell me what to drop.
- `llms.txt` and `llm.txt`: same format (title, quote summary, link lists), V3 summary, new links (how-it-works, faq,
  guides, t-pose-rig, image-to-rigged-3d-character), category list, languages. No «V2», OpenPose, T-pose preparation,
  Blender/Unity version numbers (grep: only the URL `/t-pose-rig` contains «t-pose»).
- `R:\autorig\skill.md` (repo root, outside autorig-online) is an older, different copy and is only the third fallback
  path; I left it alone (delete it or sync it).

## 2. sitemap and robots
`/sitemap/pages.xml` is the static file `static/sitemap-pages.xml` (read from the live release by `sitemap_pages`), so I
edited it. Before: 86 URLs, 82 x 200, **4 x 404**: `/ru|zh|hi|fa/developers`. Cause: `_NO_PREFIX` in
`backend/user_language.py` (~L296) contains `"/dev"` and `rest.startswith(_NO_PREFIX)` also matches `/developers`, so
every language copy of /developers is 404 although `/developers` is in `ADVERTISED_PATHS` (the pages' injected
hreflang points to them too). Fix in backend: use `"/dev/"` plus an exact `/dev` check.
Edits (83 URLs now, all 200, XML parses, no duplicates, every hreflang target is a listed URL, `/payment/success` not
listed, no «V2», checked with curl):
1. removed the 4 /developers language entries (re-add after the backend fix);
2. `/animal-rig`: removed the 7 «V2» `video:video` entries (the page no longer has those videos);
3. `lastmod` 2026-10-16 for the refreshed pages (/, /gallery, /animal-rig, /guides, /how-it-works, /faq, /t-pose-rig,
   /developers, /blender-plugin, /buy-credits, /glb-auto-rig, /fbx-auto-rig, /obj-auto-rig, skill.md, llm.txt) and for
   the /ru|zh|hi|fa copies of home, gallery, buy-credits, how-it-works;
4. the English blocks of `/`, `/gallery`, `/buy-credits`, `/how-it-works` now carry the six reciprocal hreflang links
   (the language blocks already did);
5. `/obj-auto-rig` carried hreflang to `/obj-auto-rig-ru|zh|hi` (all 404; the OBJ family is `auto-rig-obj-*`): removed;
6. added `/llms.txt` next to `/llm.txt`.
What the generator needs for the new pages (backend, not edited):
- `ADVERTISED_PATHS` and `FULL_PAGE_PATHS` (user_language.py L57-62, plus `FULL_PAGE_PATHS` in static/js/i18n.js L35):
  add `/animal-rig` (slug `animal`) and `/faq` (slug `faq`); `/guides` is already full-page but not advertised.
  Their `seo_<slug>_title/description` keys: animal exists in my pending file, faq's agent should confirm.
- then add the `/ru|zh|hi|fa` blocks to `sitemap-pages.xml`: ready blocks for /animal-rig, /faq, /developers in
  `docs/site-v3-refresh/sitemap-language-blocks.xml` (do not add earlier: until the release `/ru/animal-rig` is noindex).
- `ARTICLE_FAMILIES` (seo_social.py L86) already lists all 9 article families; no change. `/t-pose-rig` has no `-ru/-zh/-hi`
  copies (404), `/obj-auto-rig`, `/glb-auto-rig`, `/fbx-auto-rig` are single-language, so they stay out of families.
- the 12 per-animal pages are generated (main.py `RIG_ARTICLE_TYPES`) and 10 of them still print «V2» in their body text
  (bear, cow, deer, elephant, giraffe, horse, mouse, pig, rabbit, turtle — from `RIG_ARTICLE_STATIC_EXAMPLES`
  «V2» titles and «Open V2 animal rig examples»); `/rig-glb-unity` and `/rig-fbx-unreal` still say «T-pose preparation».
`robots.txt`: no change needed. Every refreshed page and article family is allowed (`Allow: /` for `*` and the AI
crawlers), `/skill.md`, `/llm.txt`, `/llms.txt` allowed, `/api/`, `/admin`, `/auth/`, `/u/`, `/m` disallowed,
`/payment/success` is not disallowed on purpose (it must stay crawlable to show its noindex), sitemap index declared.

## 3. Default OG image
`static/images/og/site.jpg` did not exist: the default card is generated by the backend, `GET /og/site.jpg`
(seo_social.py `SITE_CARD_PATH`). It is not V2 (it renders the newest V3 captures), but it is narrow: the subject filter
`SITE_CARD_SUBJECTS` only picks characters, so today it shows 5 humanoid posters (two identical) under «Auto-rig any 3D
character in about a minute»: no animals, birds, hands or cars. Suggested backend change: add animal/vehicle/hand words
to `SITE_CARD_SUBJECTS`, de-duplicate by input, tagline «Rig any 3D model, animal or car in about a minute».
Delivered: `static/images/og/site.jpg` (1200x630, 114 KB): the same card layout (logo, name, tagline, five rounded
tiles) with real V3 posters of five categories (character, horse, car, bird of prey, hand; tasks 9838cdd2, c45fcde9,
b576368c, 395b0c1d, c5c53dc6). It can be served as the fallback or the card by pointing `SITE_CARD_PATH` at it.

## Checks
- `curl` of every URL in the edited sitemap: 83 x 200; XML well-formed (`xml.dom.minidom`); every `href` of every
  `xhtml:link` is a listed `<loc>`; llm.txt/llms.txt/skill.md grep for V2/OpenPose/T-pose preparation: clean.
- Not deployed, not committed: `live_static.py put` for `sitemap-pages.xml`, `llms.txt`, `llm.txt` is enough (served from
  the live release); `skill.md` is read from the release root and needs a release.
