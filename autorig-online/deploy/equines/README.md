# Equid animation release registry

This directory is the fail-closed approval boundary for V3 hoof animation
set. It does not generate clips, post to DEV, edit Motion Transfer, or change
the current runtime by itself.

The only promotable default is the reviewed **34-action** manifest: the
authoritative `animal-base-30-v1` actions plus `canter`, `start`, `rear`, and
`tail`. The manifest, its semantic loop/frame/start-pose/end-pose contracts,
and its hash are embedded into every immutable candidate. A caller cannot add,
remove, or redefine actions. The former 14 short names remain internal aliases
for QA/integration only; a 14-clip candidate is incomplete and cannot promote.

Profiles are independent: `equid`, `cervid`, `camelid`, `bovid`, and `suid`.
Create selects one with `--profile` (default `equid`). Each profile has its own
immutable releases, mandatory server-task corpus, per-task DEV evidence, and
active/previous pointer. A release accepted for horses never applies to deer,
camels, bovines, or pigs.

## Candidate entry contract

`--entries` is a JSON array. Every entry must have the exact `clip`, `quality:
"ok"`, correct boolean `loop`, correct `type` (`loop` or `one_shot`), and
explicit QA objects shaped as `{"schema":"...","pass":true,"gates":{...},
"metrics":{...}}`. Required semantic gates vary by action; locomotion also
requires gait QA with passed `contacts` and `timing` gates. Metrics must be
finite and numeric, but numeric values alone never imply PASS.

Animation data is full 3D data under `tracks`, not sagittal samples. Each track
declares `quaternion`, `translation`, or `scale` and has one vector per frame.
All tracks must match `action_metadata.frame_count`; values must be finite and
quaternions normalized. Every clip needs quaternion and translation data,
finite positive FPS and duration, and explicit supported coordinate-system and
track-space metadata. The registry hashes the canonical full entry. Missing or
legacy `quality: null` entries cannot enter a release.

`--corpus-tasks` is a JSON array of full task UUIDs. Each task is validated
separately. `--retarget-sha256` binds the candidate to the exact retarget
implementation.

```text
python equine_library_release.py --root /srv/autorig/data/equine-library create \
  --profile equid --entries entries.json --corpus-tasks tasks.json --retarget-sha256 <64-hex>
```

## Evidence and owner verdicts

Evidence is a JSON object containing full `task_id`, `run_id`,
`rig_glb_sha256`, `release_hash`, `retarget_sha256`, a `clips` object with all
required clip names (`pass: true` plus finite numeric `qa`), and non-empty
`media_receipts`. Each receipt binds a media `uid`, the sending `agent`, and the
immutable `send_receipt`. Re-running or re-rigging a task creates a different
evidence id, so an earlier media approval becomes stale.

```text
python equine_library_release.py --root /srv/autorig/data/equine-library evidence --file task.json
python equine_library_release.py --root /srv/autorig/data/equine-library sync-verdicts \
  --agent equine-v3 --dev-api https://autorig.online/dev/api/inbox
python equine_library_release.py --root /srv/autorig/data/equine-library plan <release-hash>
python equine_library_release.py --root /srv/autorig/data/equine-library promote <release-hash>
```

`sync-verdicts` reads the real DEV row shape: a user-authored `kind: verdict`
with `Принять ✅` or `Улучшить 🔁`, whose `about.uid` and `about.agent` match a
stored send receipt. The inbox row's `inbox_id` plus `uid` is retained as the
verdict receipt. Every media UID is accepted independently and a later improve
overrides its earlier acceptance. Optional repeated `--trusted-owner-id`
arguments restrict verdicts to configured owner user IDs. Arbitrary local
approval flags are ignored. Improve, partial approval, stale media, failed QA,
or one missing mandatory task blocks promotion.

Promotion writes `active.json` atomically and retains the old pointer under
`pointers/` for undo/audit. All mutations use one lock and confined paths.
Release manifests are immutable content-addressed files.

## Runtime hook

The intended tiny `animpick` integration calls:

```python
entries = equine_library_release.resolve_runtime(ROOT, detected_species, RETARGET_SHA256)
```

It strictly classifies explicit English/Russian animal words into one profile
and returns only that profile's complete accepted immutable 34-action set.
Substring matches such as `seahorse` do not match; text naming animals from
multiple profiles is ambiguous and returns no override. Until the matching
profile is promoted, the result is empty and existing runtime behavior is
unchanged.

The production gate must run the focused test module on the VPS before this
module or its future runtime hook is installed.

## Full-3D retarget contract

`equine_retarget.py` consumes only approved entries with an explicit reference
skeleton and local absolute quaternion/translation tracks. Its complete bone
map is per target rig; a missing required bone fails the whole clip/library.
Decorative bones may be skipped only when explicitly marked optional. Morph,
scale, unknown-property and old sagittal-curve tracks fail closed.

For each bone the engine computes the source rest-relative rotation once,
changes basis through reference and target bone frames plus the declared model
forward-axis calibration, then composes the result onto the target rest
rotation. Root motion alone is rotated and scaled by target/reference height;
its target rest translation and ground baseline remain the origin. Non-root
local translations are held at target rest, preserving target bone lengths.
The engine never edits the target hierarchy, model transform, geometry,
materials, weights, or source/target inputs.

Output exposes `times`, local absolute `rotations`, all local `translations`,
and the single root as `hips_translation`, matching the current
`AP.append_clips` rotation/root-translation semantics. Metadata records pose-0
rest error, bone lengths before/after, root travel, height/ground calibration,
and named ranged options. `retarget_library` is atomic: it returns only after
every required action succeeds.

## Actual glTF source and derived target GLB

`equine_gltf_retarget.py` adapts the exact
`autorig.registry34.gltf-source-motion/1` document emitted by
`fitting_adapter/farm_export_fitted.py`. It accepts the exporter’s actual node
hierarchy, local TRS channels and accessor samples. A separate reviewed
`autorig.hoof-source-calibration.v1` document maps every retained source node
to a target bone and supplies measured model-space head/tail points, height,
ground baseline and forward axis. Nothing is inferred from Blender axes.
Non-retained Armature/ancestor nodes are not discarded: their exact static TRS
chain is composed into both the retained rest transform and every animated
sample, expressed locally to the nearest retained parent. Singular, reflected,
or shear-producing compositions fail closed; composed model translations are
never mislabeled as child-local values.

Single-action engineering candidates such as the accepted zebra Walk may be
adapted and materialized independently. They remain `candidate_not_release`;
only `retarget_library` with the exact full-34 input may represent complete
library coverage.

Python integration:

```python
entry, bone_map = equine_gltf_retarget.adapt_source_motion(source_motion, calibration)
receipt = equine_gltf_retarget.write_retargeted_glb(
    existing_target_glb, fresh_derived_glb, entry, target_rig_json, bone_map
)
```

The target must use explicit node TRS, exact rig hierarchy and static nonzero
node scales. Matrix fallback, source scale/morph channels, sparse source
animation accessors, missing mappings and nonuniform channel timing fail
closed. A target containing old animations is rejected by default; the runtime
does not activate customer/custom animation paths. The writer creates a new
GLB plus `.receipt.json`, leaves the input untouched, preserves protected JSON
objects and the original BIN as an exact prefix, then records before/after
SHA-256 values. Publication and default-library activation remain separate.
