# Face rig and T-pose vs A-pose articles (8 files) - V3 refresh, agent A, round 4, 2026-10-16

Files: `face-rig-animation[-ru|-zh|-hi].html`, `t-pose-vs-a-pose[-ru|-zh|-hi].html`. Started from the live HTML of each copy (live = local file + injected header/footer).
The index stores both families with CRLF line endings; I kept CRLF so the diff shows only real changes.

## Face rig (`/face-rig-animation`): what the live page claimed and what I did
The live page is a head-analysis tool page with a real API demo (`POST worker-0001.free3d.online/api/face-rig/analyze-head`: soft masks, 2D landmarks, region confidence).
**Over-promises found and removed/reduced (please review):**
1. Subtitle «masks ... for lip sync and expression in the AutoRig pipeline» -> now «a head analysis tool with a live demo, not a facial animation generator».
2. «Why masks ... keeps talking, blinking and emotion layers from fighting at runtime» (implies a runtime facial system) -> «masks show where eyes, mouth and brows are; AutoRig returns the masks; it does not generate blendshapes and does not capture a facial performance».
3. «After body rigging, the worker can analyze the head so you skip hand-painted weights per zone» -> «the V3 body rig includes head and neck in its skeleton; the head analysis is a separate tool».
4. A visible unfinished placeholder: «Next steps: Add engine-specific notes (Unity blendshapes, Unreal poses), capture tips ...». Replaced by «What this tool is and is not»
   (no blendshapes / morph targets, no face capture from video, no lip sync on its own). No roadmap is claimed because the live page states none.
5. Keywords «blendshapes, morph targets, lip sync, Unity/Unreal facial animation» and the og title «Face Rig & Facial Animation Guide» dropped; title «Face Rig Animation: Head Masks & Landmarks | AutoRig» keeps the keyword.
Unchanged on purpose: hero CTA, the demo markup, its CSS and JS (`node --check` OK on all four), the first insight card (input/output of the service), breadcrumb and h1 per copy.
Also: added related links (FAQ, Gallery), alt/width/height on the demo preview and chip icons, a 1200x630 OG image (the old OG image was the 259x194 demo head).
Open: the demo calls an external worker host (`worker-0001.free3d.online`), and the page text links its «pipeline tester»; I did not touch either. The zh copy now uses «蒙版» (the page's own term) consistently.

## T-pose vs A-pose (`/t-pose-vs-a-pose`)
Rewritten as an honest comparison plus «you do not need to convert»: what T-pose / A-pose mean for a rig (clear joints and symmetry vs shoulders at their extreme; A-pose closer to the working pose, helps heavy shoulder clothing),
neither is wrong, AutoRig fits the rig to the geometry in any pose, the file's own bind pose first, the reference-leg rule (standing leg sets bone lengths), skeleton offered for confirmation in blueprint mode (B) with symmetry editing, re-rig 15 s after the last edit,
the animation does not wait; «when the pose still matters» (clean pose, strongly posed models checked in the viewer). Removed: «if the tool expects T-pose, use T-pose», «Quick checklist for a good T-pose», «Free to try!».
Title keeps the keyword: «T-Pose vs A-Pose: Which Pose for Auto Rigging?». The ru/zh/hi copies were half English; now full translations.
Figure: `images/articles/pose-t-vs-a.jpg` = two gallery task posters, a T-pose racer (task 12dbfc31) and an A-pose character (task f25a3a94). OG `images/og/t-pose-vs-a-pose.jpg` uses the same two.

## SEO (all 8)
own `<html lang>`, title, description, keywords, canonical, hreflang en/ru/zh/hi/x-default (same 5 links in each copy), og + og:locale/alternates, twitter, robots, Article (dateModified 2026-10-16; datePublished kept: 2024-12-20 pose, 2026-03-24 face) + BreadcrumbList (names in the page language; face keeps its @graph with WebSite/Organization/WebPage), one h1.

## Checks
* `tools/site_page_check.py` on each of the 8 files (PYTHONIOENCODING=utf-8, no --pending): RESULT OK, 1 h1, balanced tags, JSON-LD parses, no img without alt/width/height, no missing local files, no images over 300 KB, no old wording. Descriptions 59-159 chars (zh shorter: CJK), titles <= 55.
* Internal link targets (all `/glb-*`, `/rig-*`, `/animation-retargeting*`, `/faq`, `/gallery`, `/t-pose-rig`, ...) answer 200; grep for `OpenPose|V2`: none.
* Not done: no visual render (no browser).
