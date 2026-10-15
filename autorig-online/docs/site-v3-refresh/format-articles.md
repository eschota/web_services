# Format articles refresh to V3 (page agent E, round 3, 2026-10-16)

Files (11): `glb-auto-rig`, `fbx-auto-rig`, `obj-auto-rig`, `auto-rig-obj` (+ `-ru`, `-zh`, `-hi`), `glb-vs-fbx` (+ `-ru`, `-zh`, `-hi`),
all in `static/`. Each was started from its live HTML (live = local file + injected header/footer, nothing else differed).

## Mechanism
All 11 files carry plain text, no `data-i18n` keys, and the language copies are separate files. I kept that: **no pending
i18n file** (`formats.<lang>.json` not needed). The `-ru/-zh/-hi` copies were half-translated on live (English title,
description and body mixed with the language name); they are now full translations of the English copy (title, description,
keywords, h1, body, related links, CTA, alt, breadcrumb names, JSON-LD).

## What changed
- Pages are generated from one template (scratchpad `fmt/fmt_lib.py`) in the old section layout (hero, content, related, CTA);
  page-local `<style>` adds the step cards, comparison cards, figure and article spacing (the old `workflow-steps` /
  `comparison-grid` classes had no CSS anywhere).
- V3 facts on every page: any format enters the same conveyor (GLB, FBX, OBJ, ZIP with textures); fast rig and skin in seconds,
  clips retargeted at once, quality checks in the background, the skeleton confirmation offered not required; any pose (no T-pose
  needed; the planted-leg rule; a file's own rest pose first); categories; own textures kept, PBR added for untextured models;
  OBJ without .mtl/images detected at intake and the files asked; result = V3 viewer; exports GLB, FBX, USDZ, ZIP, Unity package,
  Blender. FBX page: humanoid in Unity, skeletal mesh in Unreal. GLB vs FBX: «neither is better, same conveyor», both exports.
- Removed: T-pose requirement/«T-pose optimized/setup», «50+ animations», «Godot/CryEngine», «IK bones» claims, OpenPose/V2 (none were present).
  «Free to try» and the Offer price 0 in the SoftwareApplication node stay as on live.
- Titles keep GLB/FBX/OBJ and «Auto Rig» in the h1; one h1 per file.

## Media (real V3 only)
Pool `images/v3/`: bird-weights (GLB page), horse-028d1acd stand sheet = gallop clip (FBX page), rider-weights (OBJ page and the
OBJ guide), all in `images/articles/format-*.webp` (<= 32 KB). OG images `images/og/format-glb.jpg`, `format-fbx.jpg`, `format-obj.jpg`
(also the OBJ guide), `format-glb-vs-fbx.jpg`: 1200x630, language-neutral text, tiles from the same pool/FAQ tiles over scene renders.

## SEO
Title <= 60 and description <= 160 in every file (CJK counted by characters); keywords; canonical = own URL (the old copies had
og:url pointing at the English page); og/twitter with image, size, alt, locale; hreflang en/ru/zh/hi/x-default on the two families
(the server adds the same set only when a page has none); JSON-LD `@graph`: Article + BreadcrumbList (+ SoftwareApplication on the
three landing pages), valid; internal links in HTML (see below).

## Checks (scratchpad `fmt/chk.py`, `tools/site_page_check.py`)
- `site_page_check.py` on each of the 11 files: RESULT OK, h1 = 1.
- HTMLParser balance: no unclosed tags, no mismatches. All images have alt, width, height. JSON-LD parses in all 11.
- 42 distinct internal/static targets: all exist or answer 200 on autorig.online (incl. `-ru/-zh/-hi` siblings of the guides).
- Old-wording scan (OpenPose, V2, 50+, Mixamo, coming soon, T-pose setup): none. Latin leftovers in ru/zh/hi: technical terms only.
- Render check in headless Chrome (desktop width) for `obj-auto-rig` and `glb-vs-fbx-zh`.

## Open questions
1. OG images carry English text («GLB vs FBX», «Which format for characters?») also for the ru/zh/hi copies.
2. The three landing pages are one language (no ru/zh/hi copies exist); the families are not in `ARTICLE_FAMILIES` for them.
3. `/t-pose-rig` is linked as «Any pose, not only T-pose» (agent D reworks that page).
4. «50+ animations» was dropped because V3 does not state a clip count; say so if the number should return.
