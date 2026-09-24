# AutoRig Regen — geometry core

Splits an image-to-3D character into **body / hair / loose cloth** and builds
**bone chains** for the parts that should move (long hair, skirts, capes, coat
tails). Pure Python: numpy + scipy + trimesh (Pillow only to read a hair-mask
image). Nothing here imports the web app (`main`) or `renderfin`.

```
image A  (dressed, clean T-pose)  ──Hunyuan──►  A.glb  ──┐
image B  (same pose, skin-tight plain suit, bald) ─►  B.glb  ──┤  regen.decompose
                                                            ▼
      decomposition.json + labels.npz + weights.npz + parts.glb  ──►  Blender step
```

A is what the AutoRig converter rigs. B only serves as the reference body
underneath A's clothes and hair.

## Use

```python
from regen import decompose, DecompositionError, HairMask

try:
    result = decompose("A.glb", "B.glb", "out/", hair_mask=None, config=None)
except DecompositionError as exc:   # every expected failure; fall back to plain rigging
    log.warning("regen: %s", exc)
```

```bash
cd autorig-online/backend
python -m regen.cli decompose --dressed A.glb --body B.glb --out DIR \
    [--hair-mask mask.png [--hair-mask-bbox C0,R0,C1,R1] [--hair-mask-mirror]] \
    [--config overrides.json]
# exit 0 = ok (JSON summary on stdout), 2 = DecompositionError (reason on stderr)

python -m regen.cli synth --out DIR [--cape]     # synthetic A/B pair for smoke tests
```

`config` is a `RegenConfig` or a dict of overrides (unknown keys are an error).
All lengths in it are **fractions of the character height**.

## Coordinates

Everything is in **A's glTF space**: +Y up, lengths in A's units. Hunyuan output
faces **+Z**; the character's **left is +X** when it faces +Z. Facing is
measured (the feet point forward), so a model facing −Z also works; the
landmarks report `front_axis` / `left_axis`. Blender's glTF importer maps
glTF `(x, y, z)` to Blender `(x, −z, y)`.

## Algorithm

**Loading** (`mesh_io.py`). trimesh loads GLB/glTF/OBJ; every mesh instance
is transformed by its node matrix (mirrored instances get their winding
flipped) and concatenated, keeping per-face material ids, per-vertex UVs and
vertex colours. For topology the mesh is **welded** (`geometry.weld`): glTF
splits vertices along UV seams, which would otherwise cut every island apart.

**Alignment** (`align.py`), B → A as a similarity (uniform scale, rotation,
translation):

1. *Initial guess from landmarks clothes barely move*: feet level (min Y), the
   T-pose arm band (widest horizontal slab), hand tips (its X extremes) and
   the arms' depth. Scale = geometric mean of the span ratio and the
   shoulder-height ratio.
2. *Trimmed similarity ICP* (Umeyama) from 8k area-weighted body samples to
   150k dressed samples: each iteration keeps the best 60% of the
   correspondences (the rest are body points under loose clothes and hair),
   drops pairs whose normals disagree, and **down-weights body points that sit
   inside the dressed surface** (under tight clothes) so a shirt does not
   inflate the scale. Rotation is clamped to 10°, scale to ±30% of the guess.
3. Restarted from scale ×{1, 0.95, 1.05}; the lowest trimmed RMS wins (stops
   early once two starts agree).
4. Rejected when the trimmed RMS exceeds 2% of the height or fewer than 25% of
   the body samples lie within 1.5% of the dressed surface.

**Landmarks** (`landmarks.py`), on the aligned bald body, from 160 horizontal
slices of 60k surface samples: ground, height, arm band and span, body centre
(midpoint of the hand tips), chest half-width (torso interval under the arm
band), shoulders (torso sides at upper-arm height), neck (narrowest central
cross-section between the arm band and the widest head slice), head centre and
radius, crotch (where the gap between the legs closes, scanning up from 0.2 H),
hips (crotch + 0.05 H), facing (feet vs shins).

**Segmentation** (`segment.py`), per welded dressed vertex:

* signed distance to the aligned body: nearest of 200k body samples + body
  vertices, sign by an inverse-distance vote of the 4 nearest normals;
* `cloth` candidate: farther than **`loose_threshold` = 0.035 H** outside the
  body. Everything closer (skin, tight clothing) is `outer`;
* `hair`: vertices farther than **`hair_threshold` = 0.02 H** above the neck
  seed a flood fill over the mesh graph that may only cross vertices that are
  also that far out, inside the **head/back column** (|x − head x| ≤
  max(1.6 × head radius, 0.9 × chest half-width)) and above 0.25 H;
* optional **hair mask** (front view): every front-visible vertex (point
  z-buffer of the surface) whose pixel is inside the image is decided by the
  mask; mask hair also seeds the flood fill for what the front cannot see;
* majority-vote smoothing (2 passes) over vertex neighbours, then label islands
  smaller than 0.2% of the vertices are absorbed by their border's majority;
* `cloth` is split into connected components, largest first (`cloth_0`, ...);
  components under 0.2% of the vertices / 40 vertices / 0.04 H across are
  dropped back to `outer`;
* faces take the label of at least two of their vertices.

**Chains** (`chains.py`), per part, measured on area-weighted samples of the
part's faces (never on vertices: long triangles leave bands without any):

* attachment line = part vertices sharing an edge with `outer` (a part that
  floats next to the body falls back to its vertices nearest `outer`); heights
  come from the line's **85th percentile**, because hair lying on the back
  touches `outer` along its whole length and only its top is the root;
* attach bone: `head` for hair; for cloth by attachment height / side:
  `hips` (t < 0.4 between hips and shoulders), `spine` (< 0.72), `chest`,
  `upper_arm_l|r` (beside the torso at arm-band height), `upper_leg_l|r`
  (below the crotch, off-centre), `head` (above the neck);
* axis: vertical line through the head centre (hair) or the body cross-section
  at the attachment height nearest the attachment (torso, or one leg);
* coverage around the axis: ≥ 300° → `loop` (skirt), else `open` (cape);
  hair → `none`;
* sectors: loop 8; open one per 30° of arc, 5–7 (arcs < 90° are ribbons,
  1–3); hair circumference / 0.06 H, 6–12, even so the back centre is a
  sector centre. Sector 0 is centred on the character's front; angles grow
  toward its left;
* one chain per sector: from the sector's attachment height down to the part's
  lowest point there, `joints_per_chain` = 4 joints + an end joint at evenly
  spaced heights, each at the median radius / circular-mean angle of the part
  at that height, pushed toward the axis by 25% of the local thickness;
* sectors hanging less than 0.06 H (cloth) / 0.08 H (hair) get no chain; a
  part without chains is `rigid: true` (short hair, collars, shoes). A loop
  that loses sectors becomes the longest contiguous `open` run. Cloth around
  one leg that reaches the floor (boots, trouser legs) is always rigid;
* presets: `hair` (`hair_stiff` if the longest chain < 0.15 H), `skirt`,
  `cape`, `ribbon`.

**Weights** (`weights.py`, standalone): for each vertex, project on every
chain (extended by a virtual segment from `attach_origin` to its root), take
the two nearest chains by inverse squared distance, blend the two bones whose
segment midpoints bracket the projection, fade to the attach bone over the
first `attach_blend` = 25% of the first segment (and fully above the root),
keep 4 influences, renormalise.

## Outputs

`decomposition.json` (`format` `autorig.regen.decomposition`, `version` 1):

| key | meaning |
|---|---|
| `space`, `height` | `dressed_glb`; character height (aligned bald body, ground to head top) |
| `body_transform` | 16 floats, row-major 4×4, B.glb coordinates → A space (`body_scale` alongside) |
| `labels` | `{"values": ["outer","hair","cloth"], "file": "labels.npz", "count": N}` |
| `parts[]` | `name` (`hair`, `cloth_k`), `kind`, `vertex_count`, `face_count`, `bbox_min/max`, `attach`, `rigid`, `connection`, `hang_length`, `coverage_deg`, `attach_mode`, `attach_origin`, `attach_centroid`, `notes` |
| `groups[]` | one per non-rigid part: `name` (= part), `kind`, `part`, `attach`, `connection` (`none`/`open`/`loop`), `preset`, `chains[]` (`joints` root→end incl. the end joint, `bones` named `<group>_<cc>_<j>` … `<group>_<cc>_end`, `angle_deg`, `length`), `attach_origin`, `axis` ([x, z]), `weights` |
| `landmarks` | `ground_y`, `height`, `neck`, `head_center`, `head_radius`, `hips`, `shoulder_l/r`, `crotch_y`, `arm_band_y`, `front_axis`, `left_axis`, `warnings`, … |
| `diagnostics` | `align_rms` (dressed units), `align_rms_relative`, `align_inlier_ratio`, `align_scale`, `align_rotation_deg`, `label_counts`, `hair_source`, `weights` stats, `warnings`, `timings` |
| `config` | every threshold used |

`attach_origin` is the point a group hangs from (hair: head centre; cloth:
body axis at the attachment height) and is what `compute_chain_weights`
expects; `attach_centroid` is the literal centroid of the attachment line.

`labels.npz`: `positions` float32 (N×3, A's vertices in A's flattened order),
`labels` uint8 (index into `values`), `part_index` int16 (−1 = outer),
`signed_distance` float32. **Match rows to another mesh by position, not by
index** — any importer may reorder or split vertices.

`weights.npz`: per group `<name>__vertices` (int32 rows of labels.npz),
`<name>__bone_ids` (int32, N×4; 0 = attach bone, then chain joints in chain
order, end joints excluded), `<name>__weights` (float32, N×4, rows sum to 1).

`parts.glb`: `outer`, `hair`, `cloth_k` (A's faces with A's materials and
UVs; a layer spanning several materials adds `<name>__mat1`, … under its
node), `body_inner` (aligned B moved inward along its normals by 0.004 H plus
however far it pokes out of A, at most 0.03 H), and `chain_<group>_<cc>`
polylines plus `joints_<group>` points under a `debug_chains` node. If
materials cannot be re-encoded the preview is written without them (a
warning); it never fails the decomposition.

Old outputs in `out_dir` are deleted first, and `decomposition.json` is
written last (atomically), so its presence means the run succeeded.

## Failure modes (`DecompositionError`)

| message | cause |
|---|---|
| `mesh file not found` / `cannot read …` / `contains no triangles` / `unsupported mesh format` | input files (a Draco-compressed GLB needs `DracoPy`, which is not a dependency; Hunyuan does not compress) |
| `no T-pose arm band found in base body/dressed mesh: …` | not a T-pose humanoid (cube, A-pose, arms down, crouching) |
| `alignment residual too high: …` | B is not the same character / pose as A, or ICP failed |
| `… of the dressed surface is far from the aligned body` | > 80% loose: wrong pair or alignment |
| `implausible head size`, `body landmarks are out of order`, `no head found` | not a humanoid body |
| `<stage> failed: <Type>: …` | anything unexpected, wrapped so the caller can still fall back |

## Performance

Measured on 4 cores: 341k dressed + 276k body vertices in **7.4 s, 539 MB
peak RSS** (load 0.3, weld+sampling 1.0, align 1.3, landmarks 0.3, segment
2.5, chains 0.3, weights 0.2, write 1.4 s). The small test mannequin (28k
vertices) takes 1.3 s. KD-trees are built with `balanced_tree=False,
compact_nodes=False`: scipy's defaults made nearest-neighbour queries on
samples of flat faces 6–12× slower.

## Known weaknesses on real Hunyuan output, and what to tune

* **Hair touching loose clothing inside the head/back column** (long hair on a
  cape or hood) is fused into one surface by Hunyuan, so the hair flood fill
  continues into the garment's central strip. A hair mask fixes the front
  only. Fix: a colour gate on A's texture, or a back-view mask. Tune:
  `hair_column_head_scale`, `hair_min_height`.
* **Hats and big hair ornaments** above the neck become `hair` (rigid unless
  they hang).
* **Pose drift between A and B.** The edit model may move the arms a few
  degrees; beyond ~5° the hands and forearms exceed `loose_threshold` and come
  out as small cloth parts (normally rigid because they do not hang). Raise
  `loose_threshold` if a model shows sleeves-that-are-not.
* **B not fully bald or not skin-tight** makes hair/cloth coincide with the
  body: those parts silently become `outer` (rigid). Check
  `diagnostics.label_counts` against expectations.
* **Tight hairstyles** (< 2% H thick) and **thin skirts close to the legs**
  stay `outer`; flares only become cloth once 3.5% H out, so chains start
  where the garment leaves the body, not at the waistband. Lower
  `loose_threshold` (≥ 0.025, or sleeves and folds start moving).
* **Twin tails sticking out sideways** beyond the column are cut off as cloth
  parts attached to `head`.
* **Shoes/boots** are rigid only if they reach the floor around one leg;
  wide trouser legs that stop above the floor get loop chains.
* The scale has a small bias from tight clothing (≈0.1–0.5% on the test
  mannequin); `align_inside_weight` controls it.
* Thresholds that most often need tuning: `loose_threshold`,
  `hair_threshold`, `min_hang_cloth` / `min_hang_hair`, `loop_sectors`,
  `joints_per_chain`, `align_max_rms`.

## Files

| file | role |
|---|---|
| `decompose.py` | the pipeline and its outputs |
| `align.py` | pose frame, Umeyama, trimmed ICP |
| `landmarks.py` | body landmarks, body axis at a height |
| `segment.py` | signed distance labels, hair heuristic / mask, cleanup |
| `chains.py` | attachment, sectors, joints |
| `weights.py` | chain skin weights — numpy only, loadable by file path in Blender |
| `mesh_io.py` | load / flatten / layered GLB export |
| `geometry.py` | welding, sampling, normals, KD-trees, components |
| `config.py` | `RegenConfig`: every threshold, documented |
| `synthetic.py` | T-pose mannequins for tests and `cli synth` |
| `cli.py` | command line |

Tests: `autorig-online/backend/tests/test_regen_*.py` (`python -m pytest
tests/test_regen_*.py` from `backend/`).
