# /buy-credits — V3 refresh (agent B, 2026-10-16)

File: `static/buy-credits.html` (en/ru/zh/hi/fa through i18n).

## IMPORTANT: the local checkout was stale
The live page is NOT the credit-pack page that sits in the local file. Live sells **AutoRig Unlimited Monthly, $20/month**
(Gumroad subscription, `/buy-credits/checkout/autorig-unlimited-monthly`), the Blender plugin, a hidden crypto panel
(no crypto tab on live), feedback and the roadmap vote. I rebuilt the live source from the served HTML (header/footer,
canonical/hreflang/social tags, `?v=` stamps and `<html>` attributes stripped back to the template) and edited that.
Local file vs live differed in the plan section, the crypto tab, the head and the `subscription_active` JS block; the
live version is now the file. Credit-pack copy from the old local file is NOT on the page. If the owner expects the
credit packs back, that is a product decision, not a refresh.

## Kept exactly (checked by script against the live source)
Every `id`, every `href` (checkout links, `#credits-packs`, plugin), every non-i18n `data-*` attribute, all form
elements and buttons, prices ($20, "/month", the hidden crypto tiers 30/100/500/1000), all payment and vote JS.
Only three JS lines changed, all for localization: the delete-feedback `confirm()` text, the plugin button label
(`t('buy_product_plugin_cta') + price`) and the plugin price line (`t('buy_plugin_price_line', {price})`).
`node --check` on the inline scripts passes.

## What changed
* Texts to V3: hero intro, plan product copy (V3 conveyor, 3D model / picture / picture of a car), section intro, new
  plan bullet «Fast rig, animation clips and textures for every model», plugin bullet (the «V2 animal and non-humanoid
  workflows» line is out; now «humanoids, animals, creatures and other shapes», as the plugin page says).
* New block `#whats-included` («What every model gets»: fast rig, clips, textures, background checks, viewer and agent,
  downloads; nine categories in the intro; links to /how-it-works, /faq, /gallery, /developers).
* Hard-coded English made translatable: «/ month», «task unlocks», three aria-labels, feedback placeholder, plugin price
  default text, Blender logo alt.
* Roadmap titles left as they are (they are vote keys); only translated (they were English in ru/zh/hi).

## SEO
title (50) «AutoRig Unlimited Monthly ($20) and Blender Plugin»; description (158); keywords; canonical; hreflang
en/ru/zh/hi/fa/x-default; og/twitter with `static/images/og/buy-credits.jpg` (1200x630, real posters); one h1.
JSON-LD: the existing ItemList kept; Product «Unlimited Monthly» got a V3 description, an image and a
UnitPriceSpecification (20 USD, billingDuration 1, unitCode MON), facts all from the page; the plugin Offer has no price
(the assigned price loads at checkout), so none was added. Localized title/description: `seo_buy_title`,
`seo_buy_description` (zh and hi had none; ru/fa/en overridden).

## i18n (static/i18n/pending/buy.{en,ru,zh,hi,fa}.json, 46 keys each, same key set)
* changed text (7): seo_buy_title, seo_buy_description, buy_page_intro, buy_product_credits_desc,
  buy_product_credits_point_1, buy_product_plugin_point_2, buy_credits_section_intro.
* new (28): buy_plan_feature_v3, buy_per_month, buy_task_unlocks, buy_products_label, buy_payment_method_label,
  buy_crypto_close, buy_plugin_price_default, buy_plugin_price_line, buy_feedback_placeholder,
  buy_feedback_delete_confirm, buy_inc_title, buy_inc_intro, buy_inc_{1..6}_{t,d}, buy_inc_link_{how,faq,gallery,api}.
* missing translations filled (11): hi buy_page_kicker, buy_plan_feature_private, buy_plan_title,
  buy_product_credits_label, buy_product_credits_title, buy_product_plugin_label, buy_product_plugin_title, buy_title;
  roadmap_{face_rig,animals,avatar}_title in ru, zh, hi (fa already had them; repeated so the key set is equal).
* dropped: none.

## Checks
* `py -3.11 tools/site_page_check.py static/buy-credits.html --pending buy`: RESULT OK (h1 1, no unclosed tags, no
  image warnings, 77 i18n keys, 0 missing in all five languages, no «V2»).
* script: ids removed none (added whats-included, buy-includes-title); hrefs removed none; data-* identical; form elements
  identical; price/checkout strings counted equal in the body.
* JSON-LD parses; og image 1200x630; `i18n_merge.py --page buy` dry run ok (en +30 ~5, ru +28 ~10, zh +30 ~8,
  hi +30 ~16, fa +28 ~7).
* `translate_html` with merged dicts: no untranslated visible English in ru/zh/hi/fa (only the product name
  «Unlimited Monthly» stays Latin where the translation keeps the brand); titles localized.
* No browser render (rules): new block uses the page's own tokens and collapses 3/2/1 columns at 900/576 px.

## Open questions for the lead
1. The credit-pack model described in the assignment is not live; see the top. Confirm nothing else is expected.
2. The roadmap vote still offers «Animals Rig and Animation», which V3 already ships; the vote keys are backend
   wired, so I left the wording. Reword or replace the option only with the owner.
3. I did not say whether paid 3D clouds (Tripo, Hunyuan cloud) are covered by the subscription; no fact for it.
4. `alt`, JSON-LD and the OG text stay English on the language pages.
