# Code review of the V3 site refresh (agent A, 2026-10-16)

Scope: inline script/style blocks of the 55 HTML files changed since the spec commit, `static/js/header.js` and `footer.js`, and the live console of the main pages.

## Method
* Extracted all 69 inline `<script>` blocks of the changed pages: `node --check` on each (all pass), JSON-LD of every page parses.
* Static scans of the 55 files: duplicate ids, script-referenced ids that left the markup, functions/selectors/fetches that disappeared from a script (old commit vs now), CSS selector heads of every page `<style>` (all carry a page prefix; `.content-section article` and `.cta-button` are only on pages that own them, nothing matches header/footer/gallery classes).
* Live DOM scan of 53 live pages: no duplicate ids (header/footer included), every `#anchor` has a target, exactly one h1.
* Headless Chrome (own profile, CDP, no browser pane) on the LIVE URLs, 1280/1440 and 375 px: home, faq, animal-rig, guides, gallery, buy-credits, blender-plugin, developers, how-it-works, plus /fa/ and /zh/ variants, t-pose-rig, face-rig-animation, payment/success, terms, user-agreement, 4 article copies. Scripts `console_check.mjs`, `overflow.mjs`, `probe.mjs`, `verify_chrome.mjs` (scratchpad).

## Console result (live)
No uncaught exception and no failed script/CSS/image on any page. Only messages: `429 /api/support-chat/session` on guides and gallery when the pages were reloaded many times in a row (rate limit of anonymous chat sessions; not a page bug), and `404 /{ru,zh,hi,fa}/developers` (finding 5).

## Fixed
1. **blender-plugin.html (my page)**: nothing started the i18n module (no `app.js`, no `I18n.init`): verified live, `I18n._initPromise` undefined, 0 translations, the header language menu did not open and a chosen language did nothing, and the page texts/price were never translated client side. The inline script now calls `I18n.init()` and renders the price after it; it keeps the server-translated text until the translations load. Checked with the patched file in headless Chrome: menu opens, ru switch translates h1, price note and buy button, price logic intact. It was the only page (besides task-v3) with `i18n.js` but no init.
2. **header.js**
   * English-only strings: `aria-label` "Open/Close navigation" overwrote the translated label, credits label "Unlimited Monthly" / "Credits" had no key (`credits_balance` does not exist in any language), legacy markup had Russian «АДМИНКА», alt "Autorig.Online". Now `chromeT()`/`chromeSetText()` read I18n (existing `chrome_nav_open`, `credits_free`, `free3d_no_results`, `nav_*`), English stays the fallback, the nav label is re-synced on `languageChanged`. New keys: `chrome_nav_close`, `chrome_credits_balance`, `chrome_credits_unlimited`, `chrome_untitled`, `chrome_free3d_random` in `static/i18n/pending/chromejs.{en,ru,zh,hi,fa}.json`.
   * Theme toggle was inert on pages without their own script (animal-rig, blender-plugin): it flipped `body.light` and `localStorage.theme`, but the CSS and every other page use `html[data-theme]` + `autorig_theme`, so the saved theme was not even applied there. It now applies the saved theme and flips `data-theme` only when no other handler did so during the same click (checked: index with app.js, guides with its own handler, animal-rig without one: exactly one flip per click, no double toggle).
   * Free3D search results put external `title`/urls into `innerHTML` and attributes unescaped (XSS vector): now escaped.
   * Legacy `renderSiteHeader()` markup now equals the server partial (aria/i18n attributes, `type="button"`, `lang` on options); verified by comparing the normalized markup.
3. **footer.js**: the legacy footer had the old tagline, a `/buy` link and no chrome keys. `renderSiteFooter()` now returns the markup of `partials/site-footer.html` (verified equal after normalization, 32 i18n keys, `/buy-credits`). `window.SiteFooter.render` unchanged. `node --check` OK on both files; line endings kept CRLF.

## Not fixed (outside my scope), for the lead
4. **Header overflows horizontally between 1081 and about 1330 px** (CSS, present before the refresh: the partial structure is unchanged): `.header-actions` ends 14-40 px past the viewport at 1280 and 172 px at 1100, on every page (en and ru); fine at >= 1366 and at <= 1080 (mobile nav). Fix in `styles.css`: raise the mobile-nav breakpoint (the 1080 rule) to about 1340 px, or reduce `.header-inner` gap (2rem) and hide the credits label below 1400 px.
5. **`/developers` is served by nginx as a static file**: no server-rendered header/footer in the initial HTML (the SEO rule requires them), no language handling, `/ru|zh|hi|fa/developers` answer 404 while the page's own hreflang lists them. The backend fix of `/dev` vs `/developers` (user_language.py line 321) is not what answers here.
6. **/{ru,zh,hi,fa}/{faq,animal-rig,blender-plugin,t-pose-rig}** still return the chrome-scope English page with `canonical` = the English URL (verified with curl), while home, gallery, buy-credits, how-it-works are full-page localized: the backend part of commit dc7899ed (advertised paths) is not live yet (needs the backend release + restart).
7. zh/hi/fa shared JSON miss the same 20 legacy classic-converter keys (`live_stage_cl_*`, `live_cl_*`, OpenPose stages); not used by the site pages (English fallback). `buy-credits.html` no longer contains `#billing-tab-crypto`; its script uses `?.` everywhere, so it does not break (the crypto tab was removed on purpose by the page agent).

## Files changed
`static/blender-plugin.html`, `static/js/header.js`, `static/js/footer.js`, `static/i18n/pending/chromejs.{en,ru,zh,hi,fa}.json` (new; `py -3.11 tools/i18n_merge.py --page chromejs --write` merges them). Deploy: header.js/footer.js/blender-plugin.html with `live_static.py put`.
