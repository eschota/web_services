# Gallery: V3 rig-type icons and the gallery.html copy (agent B, 2026-10-16)

Nothing deployed or committed; `static/gallery.html`, `static/js/rig-icons.js` and the backend are untouched
(new files only: the seven PNGs, two tools, the gallery.html copy).

## 1. The seven icons
Existing set (`static/Icons_png/`): 1024x1024 RGBA, Human.png + 12 animals (TitleCase file names). They are not flat
silhouettes but a glass-cube illustration: hexagon (top 512,87; sides x 147..906, bottom 512,973), cyan-to-violet edge
glow with a thin white halo, bluish left wall / lavender right wall, white inner edges, a pale floor, one soft
white-lavender figure with a glowing skeleton (ring joints, cyan on the left shading to violet on the right). Alpha bbox
about 123,63..932,999.
`tools/draw_rig_icons_v3.py` draws the same frame and a figure with Pillow only (polygons, ellipses, thick polylines,
Catmull-Rom curves, 2x supersampling, gaussian glow; no AI art, no downloads) and writes
`Bird.png, Serpent.png, Monster.png, Multilimb.png, Hands.png, Vehicle.png, Other.png` (1024x1024 RGBA, 200-340 KB, alpha bbox
121,65..932,996 vs 123,63..932,999 for Human): a perched bird with a folded wing, an S-shaped serpent, a horned monster, a
spider-like multilimb body (4 leg pairs on a 4-joint spine), an open hand with per-finger joints, a car side view with wheels
(hubs and axle joints), and a faceted prop block for `other`. They are simpler than the painted originals (flat shading) but
share frame, size, halo, colours and joint style; replace them with painted art whenever you like, the keys stay.

How icons are keyed: `rig-icons.js` has `RIG_ICON_ANIMAL_KEYS` (a Set) and builds `/static/Icons_png/<TitleCase>.png?v=rigicons1`;
`humanoid` -> `Human.png`; an unknown key falls back to `Human.png`. `app.js` (1002, 1992), `task-card.js` (74) and
`gallery.html` (237) build the same URL straight from `rig_icon_key` without the list, so they work as soon as the file exists.

## `tools/patch_rig_icons_v3.py --static <static> [--backend <backend>] [--dry-run] [--no-backup]`
* `static/js/rig-icons.js`: after the 12 animal keys it adds one list entry and a comment line (exact anchor, once):
  `'bird', 'serpent', 'monster', 'multilimb', 'hands', 'vehicle', 'other',`. Checked with `node --check` before the file is replaced.
* With `--backend` (after `tools/patch_gallery_v3_categories.py` was applied): `backend/gallery_categories.py`
  `ICON_KEY_FOR_CATEGORY` maps every category to its own key (bird -> bird ... other -> other; quadruped -> dog and equine -> horse
  stay), `CATEGORIES_WITHOUT_ICON = ()`; `backend/tests/test_gallery_v3_categories.py` gets the two matching assertions.
* Exact anchors asserted once; everything checked before any write; backups `.pre_rig_icons_v3`; second run is a no-op; a
  changed anchor aborts with its name (tested).
* Tests: on a copy with the gallery patch applied, icon patch applied: `10 passed` (`py -3.11 -m pytest tests/test_gallery_v3_categories.py`);
  the unpatched gallery module is not touched when `--backend` is omitted.
Order for the lead: gallery category patch, then this one with both `--static` and `--backend`, deploy the PNGs
(`live_static.py put Icons_png/<Name>.png` x7 and `js/rig-icons.js`), restart for the backend change.

## 2. `docs/site-v3-refresh/gallery.html.v3categories`
A full copy of `static/gallery.html` with the 8 `<option>` lines (quadruped, equine, bird, serpent, monster, multilimb, hands,
vehicle, each `data-i18n="gallery_rig_type_<slug>"`, after the `humanoid` option) and the 22-slug `allowed` array
(`all, humanoid, quadruped, equine, bird, serpent, monster, multilimb, hands, vehicle, dog, bear, cat, cow, deer, elephant,
giraffe, horse, mouse, pig, rabbit, turtle`).
Byte-level check: the copy is LF like the original; a line diff shows exactly 10 changed lines (8 added options, the old
and the new `allowed` line) and nothing else; `<option>` values in the select equal the `allowed` array (22 = 22); the inline
scripts pass `node --check`; `static/gallery.html` is unchanged (sha256 d828a8b9...4cd4779, `git status` clean).
To apply: `copy docs\site-v3-refresh\gallery.html.v3categories static\gallery.html`, then
`i18n_merge.py --page galleryfilter --write` (the eight option labels).

## Open points
1. The painted originals are rendered illustrations; mine are programmatic. If you want art-level icons, give the Human/Dog
   prompt and generate Bird/Serpent... with the same pipeline, then overwrite the PNGs.
2. `other` has an icon but the filter does not offer it (no `other` option); it is only used on cards.
3. rig-icons.js and the direct URL builders share `?v=rigicons1`; the new files have never been requested, so no stale cache.
