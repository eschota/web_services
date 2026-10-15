# Site refresh to V3 — summary (2026-10-16)

## Для владельца (кратко)

Весь сайт autorig.online переписан под V3: главная, как это работает, FAQ, галерея, гиды, страницы животных, плагин Blender,
покупка, API для разработчиков, 39 статей (форматы, Unity/Unreal, Mixamo, ретаргетинг, лицо, T-/A-поза, картинка в персонажа),
шапка и подвал, а также файлы для роботов (`skill.md`, `llm.txt`, `llms.txt`, sitemap, OG-карточки). Тексты написаны по фактам
спеки (любой вход, категории включая машины/руки/птиц/лошадей, вьюер, агент, экспорт), без V2/OpenPose/T-pose-обязательности; на
всё есть переводы en/ru/zh/hi/fa и настоящие V3-картинки (без стока и генерации). Живой аудит (107 страниц) сейчас показывает
12 жёстких находок: 10 страниц животных со словом «V2» и `/developers` без серверной шапки — оба исправлены в репозитории и
выйдут с бэкенд-релизом `sitev3b`; после него появятся индексируемые языковые версии `/ru /zh /hi /fa` для faq, animal-rig,
blender-plugin, t-pose-rig, guides и developers. Отдельным релизом пойдут категории V3 в галерее (смена схемы БД + backfill).
Юридические тексты (Terms, User Agreement) не менялись: их формулировки — решение владельца (см. «Known leftovers»).

## 1. What was touched

Spec: `docs/site-v3-refresh-spec-20261016.md`. Each row has a report in this folder (named in the last column when it exists).
Keys = new/changed i18n keys in the page's `static/i18n/pending/<slug>.<lang>.json` (same set in en/ru/zh/hi/fa), merged into
the shared files by `i18n_merge.py`. "Plain" = the page carries its own text per language file, no i18n keys.

| Path / family | What changed (one line) | i18n keys | Media added | Commit | Report |
|---|---|---|---|---|---|
| `/` (index.html) | V3 hero and single upload zone copy, 7 category cards, new pipeline (V2 tags gone), FAQ block, JSON-LD | 96 (`home_*`) | 14 images + OG | fc117c7c | home.md |
| `/faq` | 12 sections, 51 questions on the V3 viewer, agent, categories, cars, textures, exports; FAQPage JSON-LD | 153 (`faq_*`) | 12 images + OG | fc117c7c | faq.md |
| `/how-it-works` | Whole flow rewritten to V3 (7 steps, viewer, categories, vehicle conveyor), HowTo JSON-LD | 149 (`hiw_*`) | 28 images + OG | d7f06be7 | how-it-works.md |
| `/animal-rig` | Six animal categories, equine template, birds, clip library, quality gates, gallery strip, 12 per-animal links, FAQ | 167 (`animal_*`) | 19 images + OG | c381b3f8 | animal-rig.md |
| `/developers` | V3 task flow card, typical flow, endpoint table and curl examples kept byte for byte | 20 (`dev_*`) | OG | c381b3f8 | developers.md |
| `/blender-plugin` | Plugin as the Blender-side client of the V3 conveyor, V2 demo frames removed, live checkout/price facts kept | 70 (`plugin_*`) | 5 images + OG | c381b3f8 | blender-plugin.md |
| `/buy-credits` | V3 copy on the live Unlimited Monthly page (local checkout was stale), live checkout facts kept | 46 (`buy_*`) | OG | c381b3f8 | buy-credits.md |
| `/gallery` | New h1/intro/empty state, About section, SEO + CollectionPage; script, ids and filter values untouched | 33 (`gallery_*`) | OG | dc7899ed | gallery.md |
| `/guides` | V3 index: "by what you rig" (7), "inside the viewer" (6), the 13 rig-type cards kept | 56 (`guides_*`) | 8 images + OG | dc7899ed | guides.md |
| `/t-pose-rig` | Became "any pose works": bind pose from the file, reference-leg rule, offered confirmation | 67 (`tpose_*`) | 4 images + OG | dc7899ed | t-pose-rig.md |
| Format articles (11): `glb-auto-rig`, `fbx-auto-rig`, `obj-auto-rig`, `auto-rig-obj`+3, `glb-vs-fbx`+3 | V3 text, full ru/zh/hi translations (old copies were half English), Article JSON-LD | plain | `articles/format-*`, 4 OG | dc7899ed | format-articles.md |
| Engine articles (8): `rig-glb-unity`+3, `rig-fbx-unreal`+3 | V3 text, any pose, exports; full translations | plain | 2 figures, 2 OG | dc7899ed | rig-engine-articles.md |
| Mixamo + retargeting (8): `mixamo-alternative`+3, `animation-retargeting`+3 | Rewritten per language from one structure: categories, clip library, retarget with QA | plain | 6 cat images + weights/clips, OG | dc7899ed | mixamo-retargeting-articles.md |
| Face + pose articles (8): `face-rig-animation`+3, `t-pose-vs-a-pose`+3 | Honest head-analysis scope (over-promises removed), any-pose article | plain | 6 images, 6 OG | a7d03d9f | face-pose-articles.md |
| Image to character (4): `image-to-rigged-3d-character`+3 | Picture → model → textures → V3 rig → viewer, car-picture path; live pricing/batch sections kept | plain | reuses `articles/`, OG | ab594d91 | image-to-character.md |
| Legal: `/terms`, `/user-agreement`, `/payment/success` | Head-only SEO (description, canonical, OG, JSON-LD); `/payment/success` noindex; bodies byte-identical | 0 | — | 5c36681a | legal-pages.md |
| Header + footer partials | V3 tagline, Services/Guides/Company columns, every string on a key, ru/zh/hi/fa fixes | 20 (`chrome_*`) | — | 6e4435d2 | chrome.md |
| Machine files | `skill.md`, `llm.txt`, `llms.txt` rewritten to V3; sitemap cleaned (no 404 copies, no V2 videos, lastmod, hreflang); default OG card | — | `og/site.jpg` | 20d627e8 | machine-files.md |
| Localization pass | 186 keys fixed/added in five languages (English leftovers, viewer/live lines for zh/hi/fa, V2 wording, SEO lengths); `/guides` and `/t-pose-rig` advertised | 186 | — | 928c4732 | localization-pass.md |
| Backend: language paths | `/faq`, `/animal-rig`, `/blender-plugin`, `/t-pose-rig` as full translated pages (pending release) | — | — | dc7899ed | localization-pass.md |
| Backend: `/dev` prefix | `_NO_PREFIX` matched `/developers`; now `/dev/` plus exact `/dev` | — | — | 0c094996 | machine-files.md |
| nginx `/developers` | Alias to the raw file removed so the backend renders it (layout, localization, language copies) | — | — | d782dbe4 | live-audit-2026-10-16.md |
| Per-animal pages (13, backend) | Anchored patch: V3 category texts in en/ru/zh/hi, no V2, descriptions ≤ 154 | — | — | 4cb3ba0f | rig-articles-backend.md |
| Gallery V3 categories (backend) | Anchored patch + tests + 8 filter keys (`galleryfilter`); not applied | 8 | — | 25f57329 | gallery-backend.md |
| Tooling | spec + `i18n_merge.py`, `site_page_check.py` + media pool index, `site_live_audit.py` | — | `images/v3/` pool | 18283378, ba24f839, 387c4c0f | — |

Totals: 22 article images, 31 OG cards (1200×630, `static/images/og/`), 39 article files, 877 page keys in the pending files of the eleven pages and header/footer (plus the 186 of the localization pass).

## 2. Tools

All run from `R:\autorig\autorig-online`, Python 3.11, stdlib only. No tool deploys or commits.

| Tool | Usage (one line) |
|---|---|
| `tools/site_page_check.py` | `py -3.11 tools/site_page_check.py static/<page>.html [--pending <slug>] [--live /path]` — parse, one h1, SEO fields, JSON-LD, local targets, i18n coverage (shared + pending files), old wording; before a deploy of a page. |
| `tools/i18n_merge.py` | `py -3.11 tools/i18n_merge.py [--page <slug>] [--write] [--drop k1,k2]` — merges `static/i18n/pending/<slug>.<lang>.json` into the five shared files (dry run without `--write`; reports overrides). |
| `tools/site_live_audit.py` | `py -3.11 tools/site_live_audit.py [--only <text>] [--base URL] [--no-assets]` — ~40 s, audits every sitemap URL + language variants (status, lang/dir, title/description, canonical, hreflang reciprocity, robots, OG, JSON-LD, h1, banned wording, static links, layout); writes `live-audit-<date>.md/.json`; exit 1 on hard failures; "pending" = known open item. In Git bash write `--only animal-rig` without a leading slash. |
| `tools/patch_rig_articles_v3.py` | `py -3.11 tools/patch_rig_articles_v3.py <main.py> [--dry-run] [--no-backup]` — 31 exact anchors for the per-animal pages' texts; asserts each once, `py_compile`s, backup `main.py.pre_rig_v3`, re-run is a no-op. Test: `backend/tests/test_rig_article_v3_texts.py`. |
| `tools/patch_gallery_v3_categories.py` | `py -3.11 tools/patch_gallery_v3_categories.py <backend dir> [--dry-run] [--no-backup]` — 9 anchors in 4 files + new `gallery_categories.py`, backups `*.pre_gallery_v3`. Test: `backend/tests/test_gallery_v3_categories.py` (10 tests). Backfill: `gallery_categories.py --backfill [--dry-run] [--limit N]`. |
| `static/images/v3/README.md` | Index of the real V3 media pool (posters, bird/horse/hand sheets, scene renders) used for all pages. |

## 3. Pending backend releases

**Release `sitev3b` (language paths and the rest of the refresh).** Unlocks:
- Language paths: `/ru|zh|hi|fa/` for `/faq`, `/animal-rig`, `/blender-plugin`, `/t-pose-rig`, `/guides` become real, indexable pages
  (self canonical, `<html lang>`/RTL for fa, hreflang cluster). Today they are noindex copies of the English page (audit: "pending").
  Then add the language blocks to the sitemap (`docs/site-v3-refresh/sitemap-language-blocks.xml`, ready) and remove
  the pending entries from `KNOWN_PENDING` in `site_live_audit.py`.
- `/dev` prefix fix (0c094996) plus the nginx change (d782dbe4): `/ru|zh|hi|fa/developers` stop answering 404 and `/developers` gets the
  server-rendered header/footer and localization (the only hard `chrome_missing` finding today).
- Per-animal texts (4cb3ba0f): the 10 per-animal pages stop printing "V2" (the other hard findings), descriptions ≤ 154 chars (13 are over 160 today).
- `skill.md` (read from the release root) and `/llm.txt` (served by the backend from its release): V3 versions go live (`llms.txt` and the
  sitemap already are).
- Header/footer partials (6e4435d2): the backend serves partials from its own release dir, so the V3 chrome shows after the restart.
After the release: run `site_live_audit.py`, expect 0 pending and 0 hard failures.

**Gallery categories release (after sitev3b).** `patch_gallery_v3_categories.py`: schema change
`tasks.v3_category VARCHAR(16)`, `tasks.v3_subcategory VARCHAR(48)` + index (added by `init_db` at the next start of autorig-storage),
then the one-off backfill of finished V3 tasks. Unlocks: `category` on every gallery item, an exact SQL filter (offset/total/has_more),
old animal slugs kept (`rig_type=horse` returns real horses), V3 options in the filter. Then the lead edits `static/gallery.html`
(8 `<option>`s and the `allowed` array, snippet in `gallery-backend.md`), merges the `galleryfilter` keys, and adds the missing icons.

## 4. Known leftovers

- `/payment/success`: 9 hard-coded English strings (title, unlock button, status, JS messages); keys `payment_unlock_*` exist, the page body must be
  edited (body was kept byte-identical on purpose). Terms §2 and User Agreement §1 still describe "3D character rigging" only: owner's call.
- `viewer_*` keys ownership: 81–83 Unity-viewer strings (plus 77 `live_*`) are unreferenced in this repo; they were English-only for zh/hi/fa (filled
  by the localization pass). Who owns them (viewer build, MT code, Telegram) is open; do not drop them blindly.
- 142 drop-candidate i18n keys (`drop_candidates` in `localization-pass-keys.json`, ready `drop_command`; 444 unused, 204 "verify before dropping"). Nothing deleted.
- zh meta descriptions under 70 characters on 9 pages (`/zh/`, `/zh/gallery`, `/zh/how-it-works`, `/zh/guides`, the zh copies of rig-glb-unity,
  rig-fbx-unreal, t-pose-vs-a-pose, face-rig-animation, image-to-rigged-3d-character).
- 13 per-animal descriptions over 160 characters (humanoid 177, dog 171, …): fixed by the per-animal patch.
- Gallery thumbnails (720×1280) used as OG images on the humanoid, dog and cat pages, and old V2 posters on horse, rabbit, turtle
  (`og_small` warnings): needs the per-animal patch plus 1200×630 cards.
- Missing gallery icons: `Bird, Serpent, Monster, Multilimb, Hands, Vehicle` in `static/Icons_png/` and `RIG_ICON_ANIMAL_KEYS` in `rig-icons.js`.
- No `-fa` versions of the article families (they exist as `-ru/-zh/-hi`); fa users get the English article through the chrome only.
- Image `alt` text is not translated server-side (`_ATTR_KEYS` knows placeholder/title/aria-label only); `/animal-rig` and `/gallery` apply
  `data-i18n-alt` with a small script; JSON-LD and OG image text stay English on language pages.
- Other items from the reports: `footer.js`/`header.js` still hold old English strings and an old footer copy; 9 operational tool pages have no
  footer; the default `/og/site.jpg` card (backend) shows characters only (`SITE_CARD_SUBJECTS`); `R:\autorig\skill.md` is a stale copy; the buy-credits
  roadmap vote still lists "Animals Rig"; unused plugin frames dir `static/images/plugin/plugin-short-frames-20260516/`; face-rig page over-promises
  were reduced (review); `image_gen_subtitle` wording and the 20 classic `live_*` lines (decide with the MT owner); the credit-pack copy is not on the
  live buy page (product decision).

## 5. Re-run checklist after a deploy

1. `py -3.11 tools/site_live_audit.py` → read `live-audit-<date>.md` (summary line first).
2. A changed page: `py -3.11 tools/site_page_check.py static/<page>.html --pending <slug>`, merge with `i18n_merge.py --page <slug> --write`.
3. Static files go live with `sudo python3 /srv/autorig/tools/live_static.py put <rel> <file>`; backend files (partials, `skill.md`, `main.py`) need a release.
