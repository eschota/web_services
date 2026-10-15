# Per-animal rig pages (backend-rendered): V3 text patch (agent B, 2026-10-16)

Prepared only: nothing deployed, committed or applied to `backend/main.py`.

## Deliverables
* `tools/patch_rig_articles_v3.py`: anchored patch, `py -3.11 tools/patch_rig_articles_v3.py <main.py> [--dry-run] [--no-backup]`.
  31 anchors, each exact text asserted to occur exactly once (no line numbers); CRLF/LF preserved; the result is written
  to a temp file, `py_compile`d, then moved over main.py (backup `main.py.pre_rig_v3`); a re-run is a no-op
  ("already patched"); a changed anchor aborts with its name and touches nothing (tested).
* `backend/tests/test_rig_article_v3_texts.py`: new test (no existing test covered these pages). It `exec`s only the
  rig-article block of main.py (main.py itself cannot be imported here: no `slowapi`); env `RIG_ARTICLE_MAIN_PY` points
  it at another copy. It fails on the current text and passes after the patch.

## What the patch changes (text only)
Keys, function signatures, routes, `RIG_ARTICLE_INDEXED_LANGS`, the title/h1 mechanism, JSON-LD and the examples wiring
are untouched. Title and h1 per language stay as they are (title <= 58 chars for all pages).
1. `RIG_ARTICLE_LANGS`: the "Online workflow" heading becomes "How it works" (ru, zh, hi translated).
2. `RIG_ARTICLE_TYPES["humanoid"]["tip"]`: "Use a clear T-pose or relaxed A-pose..." becomes "Any pose works, because the rig
   is fitted to the model as it is..." (4 languages).
3. New constants `RIG_ARTICLE_V3_CATEGORIES` / `RIG_ARTICLE_V3_CATEGORY_BY_KEY` and helper `_rig_article_v3_category`:
   quadruped for dog, bear, cat, cow, elephant, giraffe, mouse, pig, rabbit; the equine template for horse and deer
   (scapula, elbow, mid-neck, leg profile joints, planted-hoof gaits walk/trot/gallop); turtle = low-body quadruped with
   "no separate turtle template"; biped (incl. primates, reference-leg rule) for the humanoid page. There is no bird page
   among the 13, so birds are not mentioned.
4. `_rig_article_text`, every language (en, ru, zh, hi): description (<= 154 chars), lead (the original shape sentence is
   kept, then the V3 category sentence and "skeleton and skin in seconds, clips at once, one viewer"), workflow (upload,
   fast rig, clips at once, skeleton offered and never required, textures, background checks, downloads GLB/FBX/USDZ/ZIP/
   Unity package/Blender) and the three FAQ answers (questions unchanged). No V2/OpenPose/T-pose-required wording, no
   promised times (the only "seconds" is the rig build from geometry, as in the V3 facts).

## Before / after: /horse-auto-rig (English)
BEFORE
* description (164 chars, over the limit): Online AI auto rigging for horse 3D models. Upload GLB, FBX, or OBJ and export a rigged, animation-ready result for Blender, Unity, Unreal Engine, and web previews.
* lead: AutoRig.online automatically builds rigs for horse models: equine quadrupeds with long legs, hooves, mane, tail, and gait-focused deformation. This page targets creators who need a fast online workflow for a specific rig type, not only generic humanoid character rigging.
* workflow: Upload a GLB, FBX, or OBJ file, wait for the AI-assisted rigging task, open the task preview, and export the rigged result for Blender, Unity, Unreal Engine, or web viewers. The workflow is designed for horses, ponies, mounts, unicorns, and stylized riding animals.
* FAQ 1: Yes. AutoRig.online accepts GLB, FBX, and OBJ uploads and runs an AI-assisted rigging workflow for horse models when the geometry matches the supported rig type.

AFTER
* description (146 chars): Auto rig a horse 3D model online: rig in seconds, clips at once, a viewer with blueprint mode, GLB/FBX exports for Blender, Unity and Unreal.
* lead: AutoRig.online rigs horse models on one conveyor: equine quadrupeds with long legs, hooves, mane, tail, and gait-focused deformation. The rig uses the equine template: scapula, elbow, mid-neck and leg profile joints, and planted-hoof gaits (walk, trot, gallop) that keep the hooves on the ground. The skeleton and the skin are built in seconds, animation clips are retargeted at once, and everything opens in one viewer.
* workflow: Drop a GLB, FBX, OBJ, a ZIP with textures or a picture on the upload zone. The fast rig builds the skeleton and the skin in seconds, and clips from the category library are retargeted at once. The viewer offers to confirm the skeleton, which is never required, textures a model that has none, and runs quality checks in the background. Then download GLB, FBX, USDZ, a ZIP, a Unity package or a Blender file for Blender, Unity, Unreal Engine or the web. The workflow is designed for horses, ponies, mounts, unicorns, and stylized riding animals.
* FAQ 1: Yes. Upload a GLB, FBX or OBJ, a ZIP with textures or a picture. The engine picks the rig category (equine template), builds the skeleton and the skin in seconds and retargets animation clips at once.
* FAQ 2 (before: "Use a clean silhouette..."): Nothing special. The rig is fitted to the model as it is, in any pose. A clean silhouette, separated limbs and enough topology around the joints give the cleanest result, and the viewer offers to confirm the skeleton, which you can skip.

## Checks
* On a COPY of the local main.py: `--dry-run` ok; real run "31 anchors, py_compile ok"; a second run "already patched";
  a CRLF copy is patched and stays pure CRLF; a copy with one anchor altered fails with the anchor name (exit 1).
* Rendered through the module's own `_build_rig_article_html` / `_rig_article_text` (block exec, see the test): 13 pages
  x 4 languages = 52 pages: exactly one `<h1>`, title <= 60 (max 58), description <= 160 (max 154), every JSON-LD block
  parses, no `V2` / `OpenPose` / "clear T-pose" / "coming soon" wording, no "N seconds/minutes" promises, 3 FAQ items,
  the keys of the text dict unchanged, path `/horse-auto-rig-ru` unchanged.
* The unpatched copy fails the same test with 27 failing subtests (descriptions of 161-164 chars for long labels, the
  humanoid T-pose tip, old wording); the patched copy: `Ran 3 tests ... OK`.
* Spot reads of ru/zh/hi humanoid and turtle pages (ru grammar fixed: description says "для типа «{label}»"). The
  localized variants stay `noindex` as before (`RIG_ARTICLE_INDEXED_LANGS = ("en",)`).

## For the lead
Apply on the production tree with `py -3.11 tools/patch_rig_articles_v3.py <path>/main.py`, run
`py -3.11 tests/test_rig_article_v3_texts.py` there (set `RIG_ARTICLE_MAIN_PY` if needed), then the usual backend gate
and restart. The hard-coded "Open animal rig examples" (English for all languages) and the example titles are yours and
were not touched. Open question: the hi labels in `RIG_ARTICLE_TYPES` are English words ("dog और wolf"); left as is.
