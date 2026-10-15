# Layout pass over the live site (agent D, 2026-10-16)

Method: headless Chrome on the LIVE URLs (Node + DevTools protocol, one isolated browser context per job, `Accept-Language: en-US`,
theme set through `localStorage` `autorig_theme` + `theme` and forced on `<html data-theme>` / `body.light` when a page ignores it),
lazy images switched to eager, 1.5 s settle. Per run: `scrollingElement.scrollWidth > innerWidth`, elements outside the viewport
(unless clipped by an ancestor), images with `naturalWidth 0`, clipped text (`overflow:hidden` with scroll size > client size),
overlapping text boxes (non-inline, not in closed `<details>`), `href="#..."` anchors without a target, console/page errors, HTTP >= 400
from autorig.online, text contrast < 3 (warning), h1 count, header/footer presence.
Matrix (cut by the coordinator): 375x812 dark + light and 1366x768 dark for every page; the `-ru/-zh/-hi` article copies only as one copy
per language (`glb-vs-fbx-ru|zh|hi`, 375 dark); `/ru|zh|hi|fa/...` pages at 375 dark, `/fa/...` also at 1366 dark (RTL). 768 px was
dropped. `–` in the table = not in the matrix.
Noise removed from the verdicts: `429` from `/api/support-chat/session` and `/api/me/language` (rate limit hit by my own parallel load),
`.sr-only` boxes, and the footer's Product Hunt link contrast (listed once below). A first pass had overlap false positives from
closed `<details>` and wrapped inline links; the pages that showed them were re-measured with the fixed check: 0 overlaps left.
`pass*` = passes with a note. 375 px screenshots (dark and light, JPEG, full page up to 16000 px) are in the folder
`C:/Users/user/AppData/Local/Temp/claude/R--autorig/7215f104-06e0-4a63-a243-b5c1e59eb8e9/scratchpad/agentD/layout/shots375/` (file name = path with `_`, then `__dark.jpg` or `__light.jpg`); raw data in `layout/results.jsonl` and `results_run1.jsonl` beside it.

Result: no horizontal overflow, no element wider than the viewport, no broken image, no missing in-page anchor and no JS error on any
measured page at any measured viewport or theme, except the items listed below. Persian pages render right-to-left (checked by screenshot).

## Table (375 dark | 375 light | 1366 dark)
| page | 375 dark | 375 light | 1366 dark | issue |
|---|---|---|---|---|
| / | pass* | pass* | pass* | gallery cards clip 7 px (shared CSS, harmless) |
| /animal-rig | pass | pass | pass |  |
| /animation-retargeting | pass | pass | pass |  |
| /auto-rig-obj | pass | pass | pass |  |
| /bear-auto-rig | pass | pass | pass |  |
| /blender-plugin | pass | fixed (light) | pass | light theme: dark-on-dark text, fixed page-local, verified on a patched copy |
| /buy-credits | pass | pass | pass |  |
| /cat-auto-rig | pass | pass | pass |  |
| /cow-auto-rig | pass | pass | pass |  |
| /deer-auto-rig | pass | pass | pass |  |
| /developers | pass | pass | pass |  |
| /dog-auto-rig | pass | pass | pass |  |
| /elephant-auto-rig | pass | pass | pass |  |
| /fa/ | pass* | – | pass* | gallery cards clip 7 px (shared CSS, harmless) |
| /fa/animal-rig | pass | – | pass |  |
| /fa/buy-credits | pass | – | pass |  |
| /fa/developers | FAIL 404 | – | FAIL 404 | HTTP 404 (shared: backend route, see issue 1) |
| /fa/faq | pass | – | pass |  |
| /fa/gallery | pass | – | pass |  |
| /fa/guides | pass | – | pass |  |
| /fa/how-it-works | pass | – | pass |  |
| /fa/t-pose-rig | pass | – | pass |  |
| /face-rig-animation | pass | pass | pass |  |
| /faq | pass | pass | pass |  |
| /fbx-auto-rig | pass | pass | pass |  |
| /gallery | pass | pass | pass |  |
| /giraffe-auto-rig | pass | pass | pass |  |
| /glb-auto-rig | pass | pass | pass |  |
| /glb-vs-fbx | pass | pass | pass |  |
| /glb-vs-fbx-hi | pass | – | – |  |
| /glb-vs-fbx-ru | pass | – | – |  |
| /glb-vs-fbx-zh | pass | – | – |  |
| /guides | pass | pass | pass |  |
| /hi/ | pass* | – | – | gallery cards clip 7 px (shared CSS, harmless) |
| /hi/buy-credits | pass | – | – |  |
| /hi/developers | FAIL 404 | – | – | HTTP 404 (shared: backend route, see issue 1) |
| /hi/gallery | pass | – | – |  |
| /hi/guides | pass | – | – |  |
| /hi/how-it-works | pass | – | – |  |
| /hi/t-pose-rig | pass | – | – |  |
| /horse-auto-rig | pass | pass | pass |  |
| /how-it-works | pass | pass | pass |  |
| /humanoid-auto-rig | pass | pass | pass |  |
| /image-to-rigged-3d-character | pass | pass | pass |  |
| /mixamo-alternative | pass | pass | pass |  |
| /mouse-auto-rig | pass | pass | pass |  |
| /obj-auto-rig | pass | pass | pass |  |
| /payment/success | pass | pass | pass |  |
| /pig-auto-rig | pass | pass | pass |  |
| /rabbit-auto-rig | pass | pass | pass |  |
| /rig-fbx-unreal | pass | pass | pass |  |
| /rig-glb-unity | pass | pass | pass |  |
| /ru/ | pass* | – | – | gallery cards clip 7 px (shared CSS, harmless) |
| /ru/animal-rig | pass | – | – |  |
| /ru/buy-credits | pass | – | – |  |
| /ru/developers | FAIL 404 | – | – | HTTP 404 (shared: backend route, see issue 1) |
| /ru/faq | pass | – | – |  |
| /ru/gallery | pass | – | – |  |
| /ru/guides | pass | – | – |  |
| /ru/how-it-works | pass | – | – |  |
| /ru/t-pose-rig | pass | – | – |  |
| /t-pose-rig | pass | pass | pass |  |
| /t-pose-vs-a-pose | pass | pass | pass |  |
| /terms | pass | pass | pass |  |
| /turtle-auto-rig | pass | pass | pass |  |
| /user-agreement | pass | pass | pass |  |
| /zh/ | pass* | – | – | gallery cards clip 7 px (shared CSS, harmless) |
| /zh/buy-credits | pass | – | – |  |
| /zh/developers | FAIL 404 | – | – | HTTP 404 (shared: backend route, see issue 1) |
| /zh/gallery | pass | – | – |  |
| /zh/guides | pass | – | – |  |
| /zh/how-it-works | pass | – | – |  |
| /zh/t-pose-rig | pass | – | – |  |

## Files changed (page-local)
- `static/blender-plugin.html`: `/blender-plugin` was unreadable in the LIGHT theme (kicker and h1 gradient fine, but every `var(--text-*)`
  text, card and caption was dark-on-dark because the page draws its own dark background; contrast ratios 1.1-2.2). Added a 12-line block
  in its own `<style>` that pins the dark palette on `.plugin-page main` (`--text-primary/secondary/muted`, `--bg-card`, `--glass-border`, ...).
  Header and footer keep the theme. Verified on a copy of the live HTML with the block injected: no low-contrast text left, no overflow
  (screenshot `layout/bp_compare.jpg`, left live, right fixed). It is not live until you deploy the file. No id, `data-*` or script touched.
No other page needed a page-local fix.

## Issues in shared code / backend (not touched)
1. **`/ru|zh|hi|fa/developers` answer 404** (`{"detail":"Not Found"}`, no header or footer) although `pages.xml` lists them and `developers` is in
   `FULL_PAGE_PATHS`. Backend route (language-prefix handler or the sitemap): serve them, or drop the 4 URLs from `sitemap/pages.xml`.
2. **Footer Product Hunt link** (`partials/site-footer.html`, inline `color:#ff6154` on `#fff`, span "Check it out"): contrast 3.0 on every page.
   Fix: `#d4361f` or darker (>= 4.5).
3. **Gallery cards** (`css/styles.css` `.tc-card`, line 2417; seen on `/` and its language copies): the `aspect-ratio: 9/16; overflow: hidden` box clips 7 px of
   its content (456>449 and 505>498 at 375). Harmless to the eye; propose `.tc-card .tc-media { height: 100%; }` and re-check.
4. **Gallery thumbnails**: at 375 the first cards of `/gallery` render as black tiles with the placeholder logo (not a failed image: the posters of the
   latest tasks are black). A poster-generation question, not CSS.
5. **`<html dir>` stays `ltr` on `/fa/animal-rig`, `/fa/faq`, `/fa/t-pose-rig`** (chrome scope). The content is already laid out right-to-left by the pages'
   own `dir` handling, so nothing looks broken; adding `/animal-rig`, `/faq` (and `/t-pose-rig`, in the release being gated) to `FULL_PAGE_PATHS` in
   `user_language.py` and `i18n.js` would also fix `lang`/`dir` for screen readers and search engines.
6. Full-page screenshots show the fixed support-chat bubble over the right edge of content (by design, captures only).

## Not covered
768 px (dropped on request), the other 24 article language copies (one per language measured), light theme of the language copies, 1366 light,
content that appears after user interaction, the task viewer pages.
