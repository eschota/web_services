# AutoRig Regen: cloth rig step (Blender)

This step takes a character the AutoRig converter has already rigged and adds
bone chains and skin weights for its hair and loose clothing. It then exports
a cloth-ready FBX and GLB plus an AutoRig cloth manifest, which the Unity
package and the web viewer use to simulate those parts.

It runs after the decomposition (`autorig-online/backend/regen/`), which splits
the dressed Hunyuan GLB into body, hair and cloth parts and proposes a chain of
joints for each moving part.

| File | Runs in | Purpose |
|---|---|---|
| `blender_cloth_rig.py` | Blender (or any Python with the `bpy` module) | The step itself |
| `cloth_rig_core.py` | numpy only | Alignment (ICP), bone mapping, naming, chain order, collider maths, weights adapter |
| `cloth_manifest.py` | standard library | Presets, manifest builder, validator for the v1 contract |
| `cloth_rig_runner.py` | server Python (no `bpy`) | Runs Blender with a timeout and verifies the artifacts |

Blender loads the helpers **by file path** and registers each one in
`sys.modules` before running it. This follows the renderfin gotcha "A Blender
script must not import the server's package". The weights come from
`backend/regen/weights.py`, which is also loaded by path and must report
`WEIGHTS_API_VERSION == 1`.

## Inputs

- **Rigged model**: `.fbx`, `.glb`/`.gltf` or `.blend` from the converter.
  Bone names can follow any convention. Name tables cover Auto-Rig Pro (what the
  converter actually exports: `root.x`, `thigh_stretch.l`, `arm_stretch.l`,
  `head.x`, and so on), Mixamo with or without the `mixamorig:` prefix, Unreal
  and Unity (`pelvis`, `spine_01`, `thigh_l`, `upperarm_l`), Rigify `DEF-`
  bones, Biped, CC, VRoid and generic `Hips/Spine/Chest/UpperLeg.L`. When no
  name matches, the bones are mapped from the geometry of a T-pose skeleton.
- **Decomposition directory**: `decomposition.json` (format
  `autorig.regen.decomposition`, version 1, glTF Y-up "dressed_glb" space)
  plus `labels.npz` (`positions`, `labels`, `part_index`).
- **Weights module**: `backend/regen/weights.py` by default. Override it with
  `--weights-module` or `REGEN_WEIGHTS_MODULE`.

## What it does

1. **Import** the model into an empty scene. The script picks the armature
   that skins the most vertices and reads rest-pose world vertices of its
   skinned meshes, plus any meshes parented to it.
2. **Align.** Decomposition points are converted from glTF Y-up to Blender
   Z-up, `(x, y, z) -> (x, -z, y)`, exactly as Blender's glTF importer does.
   Then a symmetric trimmed similarity ICP (Umeyama, uniform scale) fits
   them to the rigged surface: a bounding-box start that matches heights and
   centres, coarse then fine passes, and the worst 15% of pairs trimmed in both
   directions. The step fails, with every candidate listed in `error.json`,
   when:
   - the residual is above `--max-residual` (default 2% of character height),
     or
   - the fit needs more than `--max-rotation` (default 20°) of rotation.

   The converter only rescales and recentres, so a large rotation means the
   decomposition is in the wrong space. A 180°/±90° turn about the vertical
   axis is tried, with a warning, only when the identity orientation fails.

   A body is nearly front/back symmetric: on the test mannequin a 180°-turned
   fit scores 0.26% against 0.23% for the true one. So when the rig has named
   L/R bones and the decomposition's shoulder landmarks are trusted
   (`facing_confident` is not false), a fit must also map the decomposition's
   left onto the rig's left.
3. **Map bones.** Humanoid slots are hips, spine, chest, upper_chest, neck,
   head, and shoulder/upper_arm/lower_arm/hand and upper_leg/lower_leg/foot/toe
   on each side. Each slot is filled in this order:
   - by name first;
   - then the spine chain from the hierarchy between the hips and the neck;
   - then anything still missing from skeleton geometry. The hips are the
     common ancestor of the lowest bone on each side and the topmost bone.
     Legs descend from the hips and arms reach sideways. The character's left
     comes from named L/R pairs, else from the decomposition's shoulder
     landmarks, else the Blender convention (facing -Y, left = +X).
   The report records which method mapped each slot.
4. **Transfer labels.** Every rigged vertex takes the label and part of its
   nearest decomposition point (`mathutils.kdtree`). Vertices more than 3% of
   height away from any point keep their weights.
5. **Build chains.** For each group whose part is not rigid, each chain
   `j0..jN` becomes deforming bones `<group>_<cc>_<k>` (from `j_k` to
   `j_k+1`) plus a non-deforming leaf `<group>_<cc>_end` at `j_N`. The chain
   root is parented, unconnected, to the mapped attach bone (falling back
   along head -> neck -> chest -> spine -> hips). `open`/`loop` groups are
   re-sorted into angular order around the attach axis; loops start at the
   character's front. Names are ASCII, unique and at most 60 characters.
6. **Reweight.** Per part, the moving vertices of all skinned meshes are
   weighted by `compute_chain_weights`. The attach origin is the centroid of
   the part's vertices that share an edge with `outer` vertices. The
   fallbacks, in order:
   - vertices that share an edge with any non-moving vertex;
   - the decomposition's own `attach_origin`;
   - the 5% of part vertices nearest the body.

   Old bone weights of those vertices are cleared, and at most 4 influences
   are written, renormalised. Bone id 0 goes to the attach bone. Other
   vertices are not touched.
7. **Colliders** (tag `body`): a head sphere; capsules for hips, spine, chest
   and upper_chest; thigh, calf, upper arm and forearm capsules on both sides.
   Bones that are not mapped are skipped. Each radius is the 60th percentile
   of the distance from the vertices dominated by that bone to the bone
   segment, measured on the half of the segment nearest each end. Unmapped
   bones (twist bones, fingers) count toward the nearest mapped segment, and
   moving-part vertices are excluded. The head sphere needs a bone above the
   head. If the head has no such child, a non-deforming leaf
   `cloth_head_top` is added.
8. **Write** these files to `--out`:
   - `<stem>.autorig-cloth.json`: the manifest, per
     `autorig-cloth/spec/cloth-manifest-v1.md`. Calibration is the hips-to-head
     distance in exported meters. Presets for the groups use built-in values,
     with radii scaled to the character's height.
   - `<stem>_cloth.autorig-cloth.json`: an identical copy named after the
     exported model, so a runtime that looks for the manifest next to
     `<stem>_cloth.fbx` finds it.
   - `<stem>_cloth.fbx`: armature and meshes, `add_leaf_bones=False`, existing
     actions baked. FBX clip names are kept stable instead of growing an
     `Armature|` prefix on each pass.
   - `<stem>_cloth.glb`: all bones, including the `_end` leaves; existing
     animations kept.
   - `cloth_rig_report.json`: the mapping, alignment, per-group
     chain/bone/weighted-vertex counts, per-part weight statistics, colliders,
     self-check, warnings and timings.
9. **Self-check**: the script re-imports both exports into a fresh scene. It
   verifies that every manifest bone exists, that each chain root's parent is
   the attach bone and each chain is linked in order, and that every weighted
   chain bone still has its vertex group.

**On any failure** the script writes `error.json` (`stage`, `error`,
`details`, `traceback`) and renames partial exports to `*.failed`. It also
writes the report with `ok: false` and calls `sys.exit(1)`. Blender exits 0
when a `--python` script raises, so never trust the exit code. Check the
artifacts, which is what `cloth_rig_runner.py` does.

## Running it

With a Blender binary:

```bash
blender --background --factory-startup -noaudio --python-exit-code 1 \
  --python autorig-online/tools/regen/blender_cloth_rig.py -- \
  --model rigged.fbx --decomposition regen_out/ --out cloth_out/ [--stem hero]
```

From the server (Python API or CLI). The runner deletes stale artifacts, runs
Blender with a timeout, writes `cloth_rig_blender.log` into the output
directory and verifies everything. It raises `ClothRigError`, with the tail of
Blender's output, when:

- `error.json` exists;
- the report is missing or not `ok`;
- the manifest fails validation;
- the GLB or FBX is missing, does not parse, or lacks a manifest bone.

```python
runner = load_by_path("autorig-online/tools/regen/cloth_rig_runner.py")  # or `import cloth_rig_runner`
result = runner.run_cloth_rig("rigged.fbx", "regen_out/", "cloth_out/",
                              blender_bin=None,   # else $REGEN_BLENDER_BIN, else `blender` on PATH
                              timeout=None)       # else $REGEN_CLOTH_RIG_TIMEOUT, default 1800 s
result.fbx, result.glb, result.manifest_path, result.report
```

```bash
python autorig-online/tools/regen/cloth_rig_runner.py --model rigged.fbx \
  --decomposition regen_out/ --out cloth_out/ --blender /opt/blender-4.3.2-linux-x64/blender
```

With the `bpy` wheel instead of a Blender binary (development and tests). It
needs CPython 3.11 exactly and `numpy<2`. With numpy 2 the wheel fails on
import with `_ARRAY_API not found`.

```bash
python3.11 -m venv venv-bpy && venv-bpy/bin/pip install bpy==4.3.0 "numpy==1.26.4" jsonschema pytest
venv-bpy/bin/python autorig-online/tools/regen/blender_cloth_rig.py -- --model ... --decomposition ... --out ...
venv-bpy/bin/python -m pytest autorig-online/backend/tests/test_regen_cloth_rig.py -q
```

Without `bpy` the same test file runs its numpy-only tests and skips the
Blender ones. Without `jsonschema` it also skips the schema check.

## Installing Blender on the Linux VPS

The farm boxes (Windows) have Blender 4.3 and 5.1, and every known-good
artifact there records 4.3.2. Use **4.3.2**: this code was developed against
the 4.3.0 `bpy` build. Blender 4.3 is a regular release, not an LTS (4.2 and
4.5 are). 5.x has not been tried.

```bash
cd /opt
curl -fLO https://download.blender.org/release/Blender4.3/blender-4.3.2-linux-x64.tar.xz
tar -xf blender-4.3.2-linux-x64.tar.xz            # ~1 GB unpacked
apt-get install -y libxi6 libxxf86vm1 libxfixes3 libxrender1 libxkbcommon0 libsm6 libgl1 libegl1
ldd /opt/blender-4.3.2-linux-x64/blender | grep 'not found'    # must print nothing
/opt/blender-4.3.2-linux-x64/blender --background --factory-startup -noaudio \
  --python-expr "import bpy, numpy; print(bpy.app.version_string, numpy.__version__)"
```

Point the service at it with `REGEN_BLENDER_BIN=/opt/blender-4.3.2-linux-x64/blender`
(for example an `Environment=` line in a systemd drop-in for the service that
calls the runner). Blender's bundled Python already has numpy, so nothing else
needs installing inside Blender.

The server side needs nothing beyond the standard library for the runner.
The weights module and the core need numpy, which Blender provides.

## Known limitations

- **Label transfer is a single nearest neighbour with no smoothing.** Labels
  can flip on part boundaries. Hidden body geometry right under a skirt can
  read as cloth. Real dressed and retopologised meshes are single outer
  shells, so the second case should not occur there.
- **The rig must be a rescaled, recentred copy of the dressed mesh.** A
  rotated or deformed rig (A-pose versus T-pose, re-posed arms) fails
  alignment by design.
- **Front/back is only guarded by the left/right check.** Without named L/R
  bones, or with `facing_confident: false`, a rig turned by 180° cannot be
  told apart from an unturned one on a symmetric body. The converter does not
  turn characters.
- **The geometric fallback assumes a T-pose.** Name mapping covers the
  converter's Auto-Rig Pro rigs, so the fallback only matters for unnamed
  rigs. The Rigify table assumes the standard human metarig.
- **Only one armature is processed.** Meshes that neither it skins nor are
  parented to it are not exported (reported as warnings).
- **Some parts are not reweighted.** Parts that are `rigid`, and part vertices
  on meshes that are not skinned, keep their weights and are reported.
- **The GLB carries at most 4 influences per vertex** (glTF). Body vertices
  with more influences are renormalised in the GLB, exactly as in any
  Blender glTF export. The FBX keeps them.
- **Verification so far** is a synthetic Mixamo-named mannequin (skirt and
  ponytail, FBX, GLB and animated inputs) with the `bpy` 4.3.0 module, plus
  name-table tests. It has not yet been run against a real converter output
  or a real Blender binary.
