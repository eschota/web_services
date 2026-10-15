# Engine articles: rig-glb-unity[-ru|-zh|-hi], rig-fbx-unreal[-ru|-zh|-hi] - V3 refresh, agent A, round 3, 2026-10-16

8 files, started from the live HTML of each copy (`curl https://autorig.online/<slug>`): live equals the local file apart from the injected
header/footer, canonical, hreflang and og:locale. The old ru/zh/hi copies were only half translated (English body under a translated title);
they are now full translations of the English copy (native wording, not literal). Built from one content source per article
(scratchpad `art/content_glb.py`, `content_fbx.py`, `build_art.py`), so the four copies have the same structure.

## What changed (all four languages)
* Titles / h1 keep the primary keywords: «Rig a GLB Character for Unity» and «Rig an FBX Character for Unreal Engine (Skeletal Mesh)».
* V3 text: one upload zone, the task opens in the viewer, fast rig in seconds, clips of the model category retargeted at once, QA in the background
  (joint placement, skin tears, limb collisions; numeric deformation checks), skeleton confirmation optional, any pose (standing-leg rule, the file's own rest pose),
  textures kept / PBR textures for untextured models, exports GLB / FBX / USDZ / ZIP / Unity package.
* Removed: «T-pose checklist / T-pose preparation / bad starting pose (not a real T-pose)», «paste a direct URL». Kept as on the live page: the Unity export «(usually FBX)»,
  the Unity-ready package remark, the Unity Inspector steps (Rig > Humanoid > Apply > Configure), the Unreal import steps (Skeletal Mesh, Import Animations), retargeting, the three common issues each.
  The live pages carry no subscription or price statements, so none were added.
* New: step «Download the Unity export», step «Test animations» (Generic for animals/birds), blueprint-mode (B) fix hint, one real V3 figure per article, 7 internal links
  (GLB/FBX auto rig, the sibling guide, pose guide `/t-pose-rig`, GLB vs FBX and animation retargeting in the matching language copy, animal rigging, how it works, FAQ).
* Each copy has its own `<html lang>`, title, description, keywords, canonical, og:locale + alternates, hreflang en/ru/zh/hi/x-default (the same 5 links in all 4 files, as the server injects them), og/twitter tags.
* JSON-LD: Article (dateModified 2026-10-16, inLanguage, publisher logo) + BreadcrumbList (Home > Guides > article, names in the page language). One h1 per file.
* Page-local `<style>` for `.article-figure` only; no shared CSS touched. No i18n keys (articles carry their own text), no pending files.

## Images (`static/images/articles/`, `static/images/og/`)
* `rig-glb-unity-weights.jpg` 800x400, from `static/images/v3/rider-weights.png` (run ede89a95, task b13b5a46): skin weights + skeleton of a biped.
* `rig-fbx-unreal-gallop.jpg` 768x572, from `horse-028d1acdb7187c532921-stand-sheet.png` (run 028d1acd): gallop clip frames of a rigged horse.
* `og/rig-glb-unity.jpg`, `og/rig-fbx-unreal.jpg` 1200x630 (headline «GLB -> Unity» / «FBX -> Unreal» + the same real picture; Latin text only, so one image serves all four languages).
* The figure alt and caption are translated per copy.

## Checks
* `py -3.11 tools/site_page_check.py static/<file> --live /<slug>` on all 8 files (no --pending): RESULT OK for each; 1 h1, balanced tags, canonical present, hreflang
  [en, hi, ru, x-default, zh], json-ld Article + BreadcrumbList parse, no images without alt/width/height, no missing local files, no images over 300 KB, no old wording.
  Titles 24-54 chars (zh 24/40 because CJK), descriptions 138-150 (zh 68/69); all <= 60 / <= 160.
* Internal link targets answer 200: all `/glb-vs-fbx*`, `/animation-retargeting*`, `/rig-*` copies, `/glb-auto-rig`, `/fbx-auto-rig`, `/animal-rig`, `/how-it-works`, `/faq`, `/t-pose-rig`.
* Grep for `OpenPose|V2` in the 8 files: none. The phrase «No T-pose preparation step» appears on purpose (negation).
* Not done: no visual render (no browser); the `.hero-features` / `.related-links` markup is the live one.

## Open questions
1. `/t-pose-rig` is the pose guide link in all languages (agent D is rewriting it as «any pose»); it has no ru/zh/hi copies.
2. `datePublished` kept at 2024-12-20 from the live JSON-LD; set another date if you prefer.
3. The Unity package / USDZ / ZIP mentions follow the spec's export list; the live articles only said «Unity export (usually FBX) plus any additional formats» and «Unity-ready package if your pipeline supports it».
