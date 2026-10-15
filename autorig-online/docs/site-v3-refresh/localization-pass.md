# Site-wide localization pass (page agent E, 2026-10-16)

State checked: checkout at commit `6e4435d2` (shared i18n merged after the header/footer round) and the live site
https://autorig.online. Nothing was edited except the pending files and this report. Scripts: scratchpad `l10n/` (analyze.py,
analyze2.py, pass2.py, rtl.py, sim.py, hrefl.py, mkpending.py). Machine-readable lists: `localization-pass-keys.json` (next to this file).

## Top findings (act on these first)
1. **`/ru|zh|hi|fa/developers` answer 404 on live**, so the whole hreflang cluster of `/developers` points at 404s. Cause in the
   backend (also in the local code): `user_language._NO_PREFIX` contains `"/dev"` and is tested with `startswith`, so
   `/developers` is treated as `/dev…` and never gets a language form. Fix: use `"/dev/"` plus an exact match for `/dev`.
2. **Live backend is older than the checkout**: `/faq`, `/animal-rig`, `/blender-plugin`, `/t-pose-rig` still answer as
   `data-i18n-scope="chrome"`, `<html lang="en">`, no `dir` (Persian is not RTL), `noindex` with canonical to the English page,
   while the pages' own hreflang clusters (faq, animal-rig) already list those URLs. This resolves with the next backend release
   (simulated below with the local code: all correct).
3. **`/guides` and `/t-pose-rig` are translated pages but not in `ADVERTISED_PATHS`**: their `/ru|zh|hi|fa/` copies stay `noindex`
   with canonical to English and carry no hreflang. Add `"/guides": "guides"` and `"/t-pose-rig": "tpose"` (the `seo_guides_*`,
   `seo_tpose_*` keys exist in all five languages).
4. **158 keys exist only in en/ru** (zh/hi/fa fall back to English): 81 `viewer_*` (Unity viewer UI) and 77 `live_*` (the live
   processing lines of `mt/static/live.js`, whose built-in fallback has only en and ru). Translated in the pending file
   (138 of them; the 20 classic-converter lines are left out on purpose, see 1.5).
5. Hard-coded English on `/payment/success` (9 strings incl. JS messages): new keys `payment_unlock_*` are in the pending file; the page must be edited.

## 1. Shared dictionaries (`static/i18n/*.json`)
Keys en/ru 2049, zh/hi 1893, fa 1891. Pages reference 1121 keys (data-i18n, -placeholder, -title, -aria-label, -alt, `t('…')`
in inline scripts in `static/*.html`, `partials/`), plus 441 more are referenced from JS/Python (static/js, backend, deploy, the MT viewer).
Every key used by a page exists in all five files (0 missing).

| check | result |
|---|---|
| used by pages, missing in zh/hi/fa | 0 |
| in en/ru but missing in zh/hi/fa (all code-used or viewer keys) | 158 each (same set); 138 translated, 20 classic lines skipped |
| keys only in zh/hi, not in en/ru | `queue_title`, `queue_waiting` (orphans; fa lacks them too) |
| zh/hi/fa value equals the English text, live keys | zh 10, hi 26, fa 0 (ru 9, mostly `guide_*_title`, `admin_*`) — all fixed in the pending file |
| mostly-Latin Hinglish in hi, live keys | `animal_variants_subtitle`, `animal_variants_preview`, `dev_flow_4` is fine (code) |
| Russian text inside en.json | none |
| Cyrillic inside zh/hi/fa | `hi.viewer_instruction` (unused key) |
| V2 / OpenPose / classic / T-pose-required wording in live keys | `image_gen_subtitle` («a T-pose or A-pose character comes back rigged; anything else comes back as a model», shown on the home upload zone), `error_classic_pipeline_off` («Classic conversion is closed…», backend error text) — rewritten in 5 languages. Every other T-pose mention in live keys is «any pose / not required» copy (tpose_*, faq_*, guides_*, home_*) |
| same wording in unused keys | 38 (hiw_openpose_*, hiw_tpose_*, hiw_unity_* HDRP, hiw_roadmap_title «Coming Soon», img2rig_h1/faq/pose* «must be a T-pose», upload_busy_tip_1, tpose_notice «Models must be in T-Pose», home_pipeline_kicker «V2 rigging pipeline», home_pipeline_tag_animals «Animal V2 rigging») |
| SEO strings over 60/160 chars | fixed: `seo_gallery_description` fa 163, `seo_hiw_title` hi 67 / fa 64, `developers_page_title` + `seo_developers_title` ru 64, `seo_developers_description` ru 163 / fa 170, `seo_faq_description` ru 175 |
| duplicate texts under several keys (live keys, >= 24 chars) | 13, e.g. `faq_q_anim_which`=`animal_faq5_q`, `plugin_link_ph`=`chrome_ph_title`, `live_qa_numeric_ok`=`live_qa_numeric_bad`, `guides_wf_tposerig_t`=`tpose_h1` (list in the json) |
| keys no page, script or backend file references | 444 (excluding the dynamic prefixes `error_`, `seo_`, `render_`, `adult_terms_`, `v3_notice_`, `task_status_`) |

Unused keys by group: `viewer_*` 83, `img2rig_*` 77, `v3_*` 51, `hiw_*` 48, `idle_ltx_*` 20, `upload_*` 18, `home_*` 13, `blueprint_*` 8, others < 10.
**Do not drop blindly:** `viewer_*`, `v3_stage_*`/`v3_sub_*`, `live_*`, `idle_ltx_variant_*` are read by the Unity viewer, the MT code or
Telegram code that builds the key at run time. `localization-pass-keys.json` has three lists: `unused` (444), `verify_before_dropping` (204)
and `drop_candidates` (142: unused with old wording, plus the dead `img2rig_*`, `hiw_*`, `home_pipeline_*` families that the page agents replaced);
the ready command is in `drop_command`. Nothing was deleted.

Classic-converter lines left untranslated (still en/ru only; `mt/live_classic.py` emits them only while the classic mirror is on, which it is not):
`live_stage_cl_*` (8) and `live_cl_*` (12), among them «Finding arms, legs and fingers» (OpenPose) and «Straightening the pose for rigging».
Recommend removing them with the classic rules in `live_classic.py`.

### Pending files: `static/i18n/pending/l10n.{en,ru,zh,hi,fa}.json` (186 keys each, same set)
- 138 missing keys (`viewer_*`, `live_*` minus classic) for zh/hi/fa; en/ru carry their current text. Placeholders `{n}`, `{name}`… checked equal to English.
- 9 new keys `payment_unlock_title/checking/return/desc/ready/signin/processing/still/refresh` (all five languages).
- The equal-to-English and Hinglish fixes (ru 7 keys, zh 10, hi 28), the two wording rewrites and the 7 SEO shortenings (the dry run below counts the changed values).
- `py -3.11 tools/i18n_merge.py --page l10n` (dry run): en +9 ~2, ru +9 ~13, zh +147 ~11, hi +147 ~30, fa +147 ~5, no check failed.
- Not fixed by key, needs a page edit: `site-footer.html` line 64 hard-codes «AutoRig.online on Product Hunt» (key `chrome_ph_title` exists);
  `dashboard.html` login link «Sign in with Google» (key `btn_login` exists); `gallery.html` initial «Page 1» / «Loading…»
  (keys `gallery_page`, `gallery_loading` exist); `payment-success.html` (the new keys). `index.html` still has «3 credits.» as the default text of
  `image_gen_subtitle` while the key says «Included with Unlimited Monthly» (the key wins after i18n runs; the HTML default is stale).
- Please confirm the new `image_gen_subtitle` wording: it assumes a picture is always rigged (spec §1: picture → 3D model → rig); the old gate text
  about T-/A-pose pictures was dropped.

## 2. Server-side translation (`translate_html` over FULL_PAGE_PATHS, 12 pages × ru/zh/hi/fa, header and footer partials injected)
- No broken markup: 0 mismatched or unclosed tags, 0 leaked `&lt;code&gt;` entities, 0 keys missing from the merged dictionary.
- Keys still English after translation: ru 3, zh 1, hi 2, fa 0 (`guide_*_title`, `developers_row_create` in hi); 0 after the pending file.
- Only `{id}` (the `/api/task/{id}` path in the developers table) shows as a brace placeholder, which is intended.
- Visible English that is not on a key (identical in all four languages): `ADMIN` (admin tab, fine), `Theme` (toggle label, replaced by an icon),
  `English` (language name, fine), `POST` (technical), `AutoRig.online on Product Hunt`, `Loading…` and `Page 1` (gallery),
  `Sign in with Google` (dashboard), three strings in `/payment/success`.
- Attributes without an i18n key (`alt`, `aria-label`, `placeholder`; the server does not translate attributes, only the alt of images with a
  `data-i18n-alt` key set by script on the pages that have it): 83 per language, mostly image `alt` on `/how-it-works`, `/guides`,
  `/blender-plugin`, `/t-pose-rig`, `/` (e.g. «The Sponza scene: …»), plus `aria-label` of the Product Hunt / YouTube links, the model
  orientation / rig type / rotate controls on `/`, and the `buy-credits` textarea placeholder. They read English to Persian/Hindi screen readers.

## 3. Persian right-to-left (live `https://autorig.online/fa/<path>`)
| page | status | dir / lang / scope | rtl.css | indexable / hreflang |
|---|---|---|---|---|
| `/fa/` , `/gallery`, `/buy-credits`, `/how-it-works` | 200 | rtl / fa / page | linked | yes, 6 alternates |
| `/fa/dashboard`, `/payment/success` | 200 | rtl / fa / page | linked | noindex (intended) |
| `/fa/guides` | 200 | rtl / fa / page | linked | noindex, canonical English, no hreflang (finding 3) |
| `/fa/developers` | **404** | – | – | finding 1 |
| `/fa/faq`, `/animal-rig` | 200 | **no dir, lang en, scope chrome** | linked | noindex (finding 2) |
| `/fa/blender-plugin`, `/t-pose-rig` | 200 | **no dir, lang en, scope chrome** | linked | noindex (findings 2, 3) |

The four chrome-scope pages already carry Persian text (6–9 k Persian characters each) but render left to right until the backend release; the
faq page has a small inline script that sets `dir=rtl` on `<main>`. Simulated with the local code (`_localize`, 12 pages × 4 languages): every page
gets `lang` = the language, `scope=page`, `dir=rtl` + rtl.css link for fa; only `/guides` and `/t-pose-rig` stay noindex (finding 3) and the two
intended noindex pages have no cluster.
Inline direction-specific CSS that will misbehave in RTL:
- `/buy-credits` (5): the two plan cards use `position:absolute; left:50%; transform:translateX(-50%)` (a centred badge: fine); a bullet list with
  `text-align:left` inside the centred card (Persian bullets will align left: change to `text-align:start`); the modal close button
  `right:0.35rem` with `padding-right:2rem` on the title (consistent, stays top-right; should be `inset-inline-end` / `padding-inline-end`).
- `buy-credits.html` line ~806 (JS template for roadmap replies): `margin-left`, `border-left`, `padding-left` indent nested replies on the wrong side in RTL.
- `/faq`: `translateX` in the arrow and the chip animations; the arrow already has `[dir=rtl]` rules, the animations are symmetric.
- No other inline `left/right/float` in the 12 pages.
Article copies without a Persian version (expected): the nine `-ru/-zh/-hi` families `animation-retargeting`, `auto-rig-obj`,
`face-rig-animation`, `glb-vs-fbx`, `image-to-rigged-3d-character`, `mixamo-alternative`, `rig-fbx-unreal`, `rig-glb-unity`, `t-pose-vs-a-pose`
(27 files). `/fa/<slug>` for each answers 200 with the English article, `noindex`, canonical to English. English-only pages with no language
copy at all: `glb-auto-rig`, `fbx-auto-rig`, `obj-auto-rig`, `lora`, `models`, `workflows`, `terms`, `user-agreement`.

## 4. hreflang consistency (live)
Crawled 55 canonical paths plus the 32 `/<lang>/` forms of the advertised pages; 67 clusters checked (target answers 200, its canonical is itself,
not noindex, links back to the page, `html lang` matches, x-default = en).
- Clean: `/`, `/gallery`, `/buy-credits`, `/how-it-works` and their language forms, and all nine article families (en/ru/zh/hi + x-default, no fa).
- **Mismatches:** `/developers`: ru/zh/hi/fa targets return 404 (finding 1). `/faq` and `/animal-rig` (and their /ru|zh|hi|fa forms): ru/zh/hi/fa
  targets are noindex, canonical to English, `html lang="en"` (finding 2; clears with the backend release).
- No cluster at all (all 200): `/guides`, `/t-pose-rig`, `/blender-plugin` (the last one will get one with the release; the first two need finding 3),
  `/payment/success` (noindex by design), the three format landing pages, `terms`, `user-agreement`, `lora`, `models`, `workflows`.

## Open questions
1. Confirm the `image_gen_subtitle` wording and whether `error_classic_pipeline_off` should be shown at all (the classic pipeline is gone).
2. Should the 20 classic-converter `live_*` lines be deleted together with the rules in `mt/live_classic.py`?
3. Who owns the Unity-viewer strings (`viewer_*`)? They are unreferenced in this repo and were English-only for zh/hi/fa.
