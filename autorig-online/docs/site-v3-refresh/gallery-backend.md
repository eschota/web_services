# Gallery V3 categories: backend patch (agent B, 2026-10-16)

Prepared only: nothing deployed or committed, `backend/main.py` untouched (all runs were on copies of the backend).

## Deliverables
* `tools/patch_gallery_v3_categories.py <backend dir> [--dry-run] [--no-backup]`: 9 exact anchors in 4 files
  (`main.py`, `database.py`, `models.py`, `v3_runtime_mount.py`), each asserted to occur exactly once, plus the new module
  `backend/gallery_categories.py` (created from the script). All results are `py_compile`d before any file is replaced
  (one failing anchor changes nothing, tested), backups `<file>.pre_gallery_v3`, CRLF kept (tested), a second run is a no-op.
* `backend/tests/test_gallery_v3_categories.py`: 10 tests, `py -3.11 -m pytest tests/test_gallery_v3_categories.py`
  (helpers, SQL filter on sqlite with the real `Task` model, pagination/total, backfill, the main.py block exec'd without
  importing the whole backend). Result on a patched copy: `10 passed`.
* `static/i18n/pending/galleryfilter.{en,ru,zh,hi,fa}.json`: 8 keys each (`gallery_rig_type_{quadruped,equine,bird,serpent,
  monster,multilimb,hands,vehicle}`); `i18n_merge.py --page galleryfilter` dry run: +8 per language, no overrides.

## Where the category comes from (findings)
* The Task table has no category column and the public task API only knows `rig_type` for the V2 animal path. The V3 run
  id of a task is in `viewer_settings.v3.dispatch_run_id` (written by `v3_runtime_mount.project_task`); `seo_social` already
  reads `<MT runs>/<run>/analysis/category.json` per request for task labels (`_v3_run_id` only works while
  `/api/mt/files/<run>/` URLs are in ready_urls, which needs_review tasks do not have).
* Cheapest reliable source: **fill a column when the run finishes** (`project_task` is the completion path: it runs on every
  callback commit and knows `record.run_id`), nothing read per request. Fallback "humanoid" in one place
  (`category_for_task`).

## Schema change and migration
`tasks.v3_category VARCHAR(16) NULL`, `tasks.v3_subcategory VARCHAR(48) NULL`, index `ix_tasks_v3_category`. No migration
framework is used here: `database.init_db` gets the lines `ALTER TABLE tasks ADD COLUMN v3_category ...` (sqlite branch and
the PostgreSQL branch with `IF NOT EXISTS`), run at the next start of autorig-storage (errors ignored when present).
Backfill of the finished V3 tasks (once, on the VPS after the restart):
`cd <release>/autorig-online/backend && sudo -u autorig /srv/autorig/venv/bin/python3 gallery_categories.py --backfill [--dry-run] [--limit N]`
(reads `viewer_settings.v3.dispatch_run_id` + category.json; never overwrites a stored value; prints seen/filled/no_run/no_file).

## What the patch changes
1. `gallery_categories.py` (new): slugs `humanoid quadruped equine bird serpent monster multilimb hands vehicle` (+ stored
   `other`), aliases for older Vision words (`adult_human`, `snake`, `insect`, `fish`, `weapon`, `prop`, `primate`...), old
   animal -> category map (dog, bear, cat, cow, elephant, giraffe, mouse, pig, rabbit, turtle -> quadruped; deer, horse ->
   equine), `category_for_run`, `category_for_task`, `icon_key_for_task`, `rig_filter_condition`, `backfill_v3_categories`.
2. `v3_runtime_mount.project_task`: when the projection is terminal, store the category (inside try/except, a log line on error).
3. `main.py`: `GALLERY_RIG_TYPES` = V3 slugs first, old animal slugs kept; `api_get_gallery` adds the SQL condition to
   `base_conditions` and switches the Python filter off, so offset/limit, `total` (the existing count query) and `has_more`
   are exact; if the column is missing (`rig_filter_condition` returns None) the old Python filter stays; each item gets
   `category`; `_gallery_rig_icon_key` returns the icon of the category for every non-V2 task.
4. `models.GalleryItem.category` (default `humanoid`).
Filter semantics: `humanoid` = stored humanoid + rows without category that are not old V2 animals (the fallback); a V3 slug =
that category + old V2 rows of the animals mapped to it; an **old slug** (`horse`) = old V2 rows of that animal + V3 rows of the
mapped category whose subcategory is one of that animal's names (same word lists the per-animal pages use), so
`rig_type=horse` no longer returns every equine.

## rig_icon_key and icons
quadruped -> `dog`, equine -> `horse`, humanoid -> `humanoid`; **missing icons** (they fall back to the Human icon, as
`rig-icons.js` does): bird, serpent, monster, multilimb, hands, vehicle (and `other`). They would be
`static/Icons_png/{Bird,Serpent,Monster,Multilimb,Hands,Vehicle}.png` plus the keys in `RIG_ICON_ANIMAL_KEYS`
(`static/js/rig-icons.js`); then edit `ICON_KEY_FOR_CATEGORY` in `gallery_categories.py`.

## static/gallery.html (frozen; the lead applies)
Line 121 stays (`humanoid`). Insert directly after it (before the `dog` option, line 122):
```
                            <option value="quadruped" data-i18n="gallery_rig_type_quadruped">Quadruped</option>
                            <option value="equine" data-i18n="gallery_rig_type_equine">Equine (horse, deer)</option>
                            <option value="bird" data-i18n="gallery_rig_type_bird">Bird</option>
                            <option value="serpent" data-i18n="gallery_rig_type_serpent">Serpent</option>
                            <option value="monster" data-i18n="gallery_rig_type_monster">Monster</option>
                            <option value="multilimb" data-i18n="gallery_rig_type_multilimb">Multilimb</option>
                            <option value="hands" data-i18n="gallery_rig_type_hands">Hands</option>
                            <option value="vehicle" data-i18n="gallery_rig_type_vehicle">Vehicle</option>
```
Line 182, the `allowed` array, becomes:
```
                const allowed = ['all', 'humanoid', 'quadruped', 'equine', 'bird', 'serpent', 'monster', 'multilimb', 'hands', 'vehicle', 'dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe', 'horse', 'mouse', 'pig', 'rabbit', 'turtle'];
```
(the old animal options stay in the select so old `?rig_type=horse` links still show a selected option; remove them from
both places later when you drop the old links). The select values, the `allowed` array and `GALLERY_RIG_TYPES` carry the
same 22 slugs. The static page text is not otherwise touched.

## Before / after: `/api/gallery?rig_type=horse`
BEFORE (live today): `{"total": 0, "has_more": false, "items": []}`; every V3 item has `rig_icon_key: "humanoid"`. The request
loads every public row (no SQL limit), filters in Python on `viewer_settings.rig_v2_animal_detection` (V2 animal path only),
then slices; 0.57 s at 800 rows, growing with the gallery. `rig_type=humanoid` returns 801 (everything).
AFTER: one SQL query with `WHERE ... AND (legacy V2 horse row OR (v3_category = 'equine' AND v3_subcategory IN ('horse','horses',
'pony','ponies','equine','unicorn')))`, `LIMIT/OFFSET` in SQL, `total` from the existing count query with the same condition,
items carry `"category": "equine"`, `"rig_icon_key": "horse"`. `rig_type=equine` returns every equine task, `rig_type=humanoid`
only humanoids and unclassified rows. Sqlite test: 12 rows -> humanoid 2, quadruped 3, equine 3, horse 2 (horse + legacy
horse, the zebra excluded), pagination `offset=1 limit=1` of equine returns the second row with total 3.

## Checks
* Patch on a copy of the local backend: dry run ok, real run ok (4 files + module), second run "already patched"; a CRLF
  copy stays pure CRLF; a changed anchor (database.py) aborts with its name and creates nothing.
* Patched copy: `py -3.11 -m pytest tests/test_gallery_v3_categories.py`: 10 passed; all four patched files compile.
* The exec'd main.py block proves `GALLERY_RIG_TYPES` has all 22 slugs, `_gallery_rig_icon_key` returns horse/dog/humanoid as
  specified, and the API source calls the SQL condition before the query is built.
* Not run: the full API (main.py cannot be imported here: no `slowapi`), a real PostgreSQL migration.

## Risks
1. `category.json` may not exist yet when the run turns terminal; then the task stays NULL (shown as humanoid) until the
   backfill. Run the backfill after deploy and, if needed, from a timer.
2. Subcategory vocabulary of the analysis is unknown to me: old-slug filters (`horse`, `dog`...) depend on it; V3 slugs do not.
3. `_rig_article_examples` uses `_gallery_rig_icon_key`: the per-animal pages now match V3 quadruped tasks for `dog` and equine
   tasks for `horse` (by keyword), `cat`, `bear`... stay on the static example; the humanoid page now lists only humanoids.
4. The ALTER runs on the big `tasks` table at start (nullable, no default: cheap on both engines); the index creation on
   PostgreSQL takes a short lock.
5. Counts in the gallery header use a separate stats query and are unchanged.
6. SSR cards (`gallery_cards_html`) still show the generic alt text; category-based alt text needs the new `category` field
   there (not part of this patch).
