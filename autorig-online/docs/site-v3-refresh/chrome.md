# Shared chrome (header + footer partials) — site V3 refresh, page agent C (2026-10-16)

Files: `static/partials/site-header.html`, `static/partials/site-footer.html` (CRLF kept),
`static/i18n/pending/chrome.{en,ru,zh,hi,fa}.json` (20 keys each).
Started from live: the header/footer extracted from `curl https://autorig.online/faq` were byte-identical to the local partials.
How they reach pages: `_inject_static_layout` (backend/main.py ~16385) swaps `<div id="site-header"></div>` /
`<div id="site-footer"></div>` for `data-server-rendered="1"` wrappers holding the partial; `localize_page` then runs
`translate_html` (data-i18n text and `data-i18n-placeholder/-title/-aria-label`), sets lang/dir on the wrappers.
`site-layout.js` only adds the `active` class on `.nav-link[href]`, `SiteHeader.init()` binds the mobile toggle, language, credits.

## Header (structure untouched)
Every id, class, data-* and the auth / credits / language / theme elements are as before (script check: nothing lost).
Nav stays Guides, Gallery, Animals, Buy, Blender Plugin, API (+ hidden ADMIN tab); nothing added. Hrefs unchanged.
Only attributes were added so that the hard-coded English moves to keys: `aria-label` of the logo (`chrome_logo_aria`),
mobile toggle (`chrome_nav_open`), primary nav (`chrome_nav_primary`); `title` of the ADMIN tab (`chrome_admin_title`) and
the theme toggle (`chrome_theme_toggle`); the language button reuses `lang_label`; the account link reuses `nav_dashboard`.

## Footer
- Brand column: V3 tagline (`footer_tagline`): «Upload any 3D model, a picture or a car picture: rigged and animated in
  about a minute, ready for Unity, Unreal and Blender.» (the one-minute target, not «seconds»).
- Services (10 links, all in the HTML): GLB / FBX / OBJ auto rigging, Animal rigging, Image to rigged character, Blender
  plugin, How it works, FAQ, Guides, Developer API (new key `chrome_footer_dev_api`).
- Guides (article families, 9 links): Mixamo alternative, GLB for Unity, FBX for Unreal, GLB vs FBX, Rigging in any pose
  (`/t-pose-rig`, new key `chrome_footer_any_pose`), T-Pose vs A-Pose, Animation retargeting, Face rig & animation, OBJ
  rigging guide (`/auto-rig-obj`, `chrome_footer_obj_guide`). The `-ru/-zh/-hi` copies of these are not linked (no hreflang pages).
- Company (4): Gallery, Buy credits (`chrome_footer_buy`; the old footer reused `nav_buy` = «Buy»), Terms, User agreement.
  The bottom legal row is unchanged (so Terms/Agreement appear twice, as a convenience; drop them here if you prefer).
- Not added, because they have no page: vehicles, hands, birds, anything «coming soon».
- Also keyed: footer logo aria (`chrome_logo_aria`), legal nav aria (`chrome_legal`), Product Hunt title and «Check it out →»
  (`chrome_ph_title`, `chrome_ph_cta`); brand names (Product Hunt, Telegram) stay literal.

## i18n
- New `chrome_*` keys (12): logo_aria, nav_open, nav_primary, theme_toggle, admin_title, legal, ph_title, ph_cta,
  footer_dev_api, footer_any_pose, footer_obj_guide, footer_buy.
- Existing keys with new/fixed text (the merge reports them as overrides): `footer_tagline` (V3, 5 languages) and the copies
  that were still English in the shared files: `nav_animal_rig`, `footer_animal_rig` (ru/zh/hi), `footer_glb_vs_fbx`,
  `footer_tpose` (ru/zh/hi), `footer_faq` (ru/hi), `nav_blender_plugin`, `footer_blender_plugin` (hi).
- Missing today: none. All 32 `data-i18n*` keys of the two partials (+ `lang_label`) exist in en/ru/zh/hi/fa. Untranslated
  (equal to English) before my fixes: the keys above; `nav_api` = «API» in all languages is intended.
- `translate_html` simulated with `backend/user_language.py` on ru and fa: tagline, nav aria, animals link, Product Hunt CTA
  come out translated.

## Checks (scratchpad `chrome_c/c_check.py`)
- HTML parser: no unclosed or mismatched tags in both partials; no id/class/data-* lost; files still CRLF.
- Pending: same 20 keys in 5 files; no Cyrillic in zh/hi/fa; ru has Cyrillic; every key used by the partials resolves in
  shared+pending for all five languages; no unused pending key.
- All 24 internal hrefs (incl. `/auto-rig-obj`, `/t-pose-rig`, `/terms`, `/user-agreement`) answer 200 on autorig.online.
- No JS touched. Not rendered in a browser (shared partials, structure unchanged, classes unchanged).

## Not in my scope, found while reading (for the lead)
1. `static/js/footer.js` still holds an old copy of the footer (legacy fallback for pages without the server partial: it
   has `/buy`, no Animal rigging, the old tagline). `static/js/header.js` (render + `initSiteHeader`) sets
   English-only strings from JS: credits label («Free conversions left» / «Credits» / «Unlimited Monthly»), the theme
   button emoji, `Open/Close navigation` aria on toggle (overwrites the translated aria after the first click),
   «АДМИНКА» in the fallback. Suggest keys `credits_*` and `chrome_nav_open/_close` in header.js.
2. 10 static pages have no `site-footer` div (3dmodel, avatars, image, nodes, rig-v2, text2text, video, viewer-v3, vision):
   operational tools, so left alone.
3. `/animal-rig` and `/faq` still wait for `FULL_PAGE_PATHS`/`ADVERTISED_PATHS` in the next backend release.
4. Merge: `py -3.11 tools/i18n_merge.py --page chrome --write`.
