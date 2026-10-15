# /developers refresh to V3 (page agent E, round 2, 2026-10-16)

Files: `static/developers.html` (started from the live HTML; it differed only in JS/CSS stamps),
`static/i18n/pending/dev.{en,ru,zh,hi,fa}.json` (20 keys each, same set), `static/images/og/developers.jpg` (1200x630, 84 KB;
real V3 tiles from `images/faq/` over the Sponza scene render, the page's own curl example).

## What changed
- Kept exactly: endpoint table (7 rows, notes text), all three curl examples, key format and key handling text, the agent
  JSON brief, the key list/generate/revoke JS, the U3D provider facts and links. Only descriptive text moved.
- Intro (`developers_intro`, changed): task = the same V3 task as on the site; result = V3 viewer with clips and exports.
- New card «How a task works in V3» (`dev_v3_*`): any input (model, picture, car picture, text; the API creates from a
  link or an upload), fast rig + clips at once + background QA, result page `/task?id=TASK_ID`, downloads (GLB, FBX, USDZ, ZIP,
  Unity package, Blender; cached-files lists every file).
- New card «Typical flow» (`dev_flow_*`): create, poll until `done`/`error`, cached-files, download. Taken from the page's
  own agent brief, nothing new documented.
- U3D card, «(revoked)» in the key list and the label «Base URL» (colon moved out of the key) are now i18n keys
  (`dev_u3d_*`, `dev_revoked`): before they were hard-coded English in every language.
- Related-pages row (HTML links: how-it-works, faq, guides, gallery, animal-rig, blender-plugin, buy-credits).
- The static defaults now equal the shared English text that i18n.js applies anyway (`developers_auth_body`,
  `api_access_title` = «API Access», `developers_agent_hint`); no text change visible live.
- No V2/OpenPose/classic wording was on the page. `type=t_pose` stays: it is the documented request parameter.

## i18n
- The 27 existing keys already had ru/zh/hi/fa. Pending: changed `developers_intro`, `developers_page_title` (the title
  element uses it), new `seo_developers_title`, `seo_developers_description` (read by the backend for the ru/zh/hi/fa head),
  and 16 new `dev_*` keys. Dropped keys: none.

## SEO
Title (56) «AutoRig Developer API: Rig and Animate 3D Models by REST»; description (160); keywords; canonical;
hreflang en/ru/zh/hi/fa/x-default (the server rebuilds it, /developers is already advertised); og/twitter with the new image;
JSON-LD TechArticle + BreadcrumbList; one h1; internal links in HTML.

## Endpoint verification (curl on https://autorig.online, plus /openapi.json)
All documented endpoints exist: `/api/task/create` (POST, 405 on HEAD, in openapi), `/api/task/{id}` and its
`prepared.glb`, `animations.glb`, `animations.fbx`, `model.glb`, `cached-files` (GET, a zero id answers 404 «Task not found»,
a made-up path under /api answers 404 too, openapi lists them all), `/api/user/api-keys` (GET 401 without a session; GET/POST
in openapi) and `/revoke`, `/api/admin/u3d-youtube/oauth/start` (401), `/dev/youtube` (200), `/dev/youtube/skill.md` (200),
`/dev/api/youtube/status` (200), `/dev/api/youtube/videos` (405 on HEAD; served by nginx from the provider, not in openapi).
Not tested (would create data or need a key): the POST calls themselves. None missing.
Note: `/dev/api/youtube/status` reports `ready:false`, `oauth_client_configured:false` right now: the provider is not ready
for uploads on prod today; the page text does not promise readiness.

## Checks
`site_page_check.py static/developers.html --pending dev`: OK (h1 1, 49 keys, missing 0 in all languages); HTMLParser balance
ok; JSON-LD and the agent brief parse; inline JS `node --check` ok; all 9 internal hrefs 200; server `translate_html` for
ru/zh/hi/fa leaves no key untranslated, no Cyrillic in zh/hi/fa; headless Chrome render at desktop and in a 375 px frame
(the endpoints table scrolls inside its own wrapper, as before).

## Open questions
1. `--live /developers` in the checker failed under Git Bash path mangling (`/Program Files/Git/developers`); compared by diff instead.
2. «Task starts from text/picture/car» is the product fact; the API doc only claims link or upload. Tell me if
   `/api/task/create` accepts a picture or a car picture so the example can say it.
