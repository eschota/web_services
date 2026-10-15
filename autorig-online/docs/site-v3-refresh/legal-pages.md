# /terms, /user-agreement, /payment/success — site refresh to V3 (agent D, round 2, 2026-10-16)

Files: `static/terms-of-use.html`, `static/user-agreement.html`, `static/payment-success.html`. Only the `<head>` of each file
was edited. Started from the live HTML (`curl -s https://autorig.online/terms|user-agreement|payment/success`).

## No legal clause changed (proof)
Before editing, the `<article>` (terms, user agreement) and `<main>` (payment success) of the live page was byte-identical to the
local file; after editing it still is. Compared with `re`: body text, every `data-i18n`, every id/class/style, and the script
list (header/footer/site-layout/i18n/app scripts and the inline payment script, version stamps ignored).

| page | body sha256 (first 12) | visible words | body = live | scripts = live |
|---|---|---|---|---|
| terms-of-use.html | 4640bbe36a9a | 763 | yes | yes (7) |
| user-agreement.html | e674edd09884 | 1049 | yes | yes (7) |
| payment-success.html | c90a74306a3a | 31 | yes | yes (5) |

Prices, refunds, liability cap (USD 50 / 12 months), jurisdiction, indemnity, licenses, effective dates: untouched.
`payment-success.html`: every id (`payment-success-desc`, `payment-unlock-recovery`, `payment-unlock-status`,
`payment-unlock-task-link`, `payment-success-actions`), the purchase-flow script (`autorigPendingDownloadUnlock`, `/auth/me`
polling, `resume_download_unlock`, dry-run query) and all inline strings are unchanged.
Note: `git diff` of `user-agreement.html` against HEAD also shows sections 6-12 (model sale offers, section 7) because the
working tree was already ahead of the last commit; that text equals production and is not from this change.

## Head diff summary
- terms-of-use: meta description now describes the V3 service neutrally («website and services: rigging, animation and exports of
  3D models, generation from a picture, and the vehicle conveyor», 155 chars; old: «automatic 3D character rigging service»);
  added keywords, canonical `/terms`, og:image (existing default `https://autorig.online/og/site.jpg`, 1200x630 + alt),
  twitter:card/title/description/image, JSON-LD `WebPage` + `BreadcrumbList`. robots stays `index,follow`; title, og:title,
  og:description unchanged.
- user-agreement: no product wording to change (the text speaks of «rigged geometry, skeletons, skin weights, animation data,
  retargeted clips» which stays correct); same additions: keywords, canonical, og/twitter image set, JSON-LD. robots `index,follow`.
- payment-success: it had no description, og:description, canonical or JSON-LD. Added description (116), canonical
  `/payment/success`, og:description, twitter set, og:image, `WebPage` JSON-LD, and `robots: noindex,follow` (the page had no robots
  meta; a post-checkout page should not be indexed. Remove that one line if you want the old behaviour).

## i18n
`py -3.11 tools/site_page_check.py <file> --pending legal`: terms-of-use uses 0 `data-i18n` keys, user-agreement 5
(`agreement_model_sales_*`), payment-success 4 (`payment_success_*`). All 9 keys already exist in en, ru, zh, hi and fa in the shared
JSON, so nothing is missing and **no `pending/legal.<lang>.json` was created** (the tool's «pending file missing» warning is
expected). The legal bodies stay English: both documents say the English version controls, and translating a clause would be a
legal change that needs the owner. The `<title>`/meta description of these pages are not localized by the server (they are not in
`SEO_KEYS`). Not localized in `payment-success.html`: the h2 «Finish unlocking your task», the unlock button and the status
strings inside the inline script; they are wired to the purchase flow, so I left them.

## Checks
- site_page_check on all three: RESULT OK (1 h1 each, canonical present, title 29/31/32, description 155/110/116, JSON-LD valid).
- JSON-LD parsed with `json.loads` (WebPage + BreadcrumbList for the two legal pages, WebPage for payment success).
- No JS touched, so no `node --check`; no images added (default OG image reused, it answers 200).

## Open questions for the lead / owner
1. Terms §2 still defines the Service as «automated 3D character rigging and related processing». It is a clause, so I did not
   change it. If the owner wants it to cover animals, hands, vehicles and generation from a picture, he must approve new wording.
2. User Agreement §1 «User Content» lists «3D models, textures, links, URLs, metadata, and other materials»; pictures of a car
   or a character are covered only by «other materials». Same rule: owner's call.
3. Add `/terms` and `/user-agreement` to `SEO_KEYS` if you want localized title/description for ru/zh/hi/fa.
