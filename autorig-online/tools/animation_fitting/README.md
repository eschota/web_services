# Animal Animation Fitting (offline v1)

This directory contains the deterministic half of the video-to-skeleton pipeline. It consumes an **actionless** AutoRig fitting bundle plus measured video observations, fits the existing animal hierarchy over time, and exports per-frame local transforms for every bone. The first executable acceptance fixture is a constrained synthetic horse body/front-leg rig.

It does not call LTX, choose the best generated video, infer a tracker threshold, invent a track-to-bone correspondence, infer foot contacts, or mark QA as passed. Those are separate upstream/review decisions. A production horse run is only valid after its real rig has local joint constraints and an explicit surface-anchor map.

## Pipeline boundary

1. `render_actionless_bundle.py` renders the default model transform and default pose with zero animation actions. Its `fitting_bundle.json`, skeleton, skin weights, anchors, camera and ground plane are the immutable rig input.
2. LTX/Comfy generates a video from the canonical render. A loop candidate must intentionally return to its starting pose; a one-shot candidate need not. The tracking runtime defaults to that canonical RGB; its v11 browser-static-scene override is opt-in, loop-only, manifest-pinned, and canonically bundle-linked as documented in [tracking_runtime/README.md](tracking_runtime/README.md).
3. A tracker/segmenter/depth model runs outside this package and exports observations. The adapters normalize point tracks; masks and calibrated depth are referenced by the canonical observation JSON.
4. This package performs bounded temporal inverse kinematics with root motion, local joint limits, observed contacts and optional loop closure.
5. The output contains raw QA measurements and every local bone matrix. A later asset-authoring step can turn those transforms into a named glTF/GLB animation clip.

The engine preserves the supplied hierarchy, helper bones, rest translations and rigid morphology. Only the root transform and selected local rotation degrees of freedom are optimized.

## Actionless bundle contract

`render_actionless_bundle.py` emits `autorig_actionless_bundle_v2`, or v3 when a semantic LTX profile is active, and fails closed unless the source scene contains exactly one armature. In Blender's temporary process it detaches object and data Actions, mutes NLA tracks, drivers and object/pose constraints, zeros and mutes non-basis shape keys, restores every animated RNA channel to its declared default, sets frame 0, resets pose bases and evaluates the armature in `REST`. The same REST assertion runs immediately before RGB/depth, semantic, silhouette and face-ID renders. The source file is never saved.

The raw vertex-ID contract is intentionally strict. Every visible mesh must have exactly one enabled Armature modifier targeting the singleton armature, no enabled geometry-changing modifier, and identical raw/evaluated REST vertex positions. Every vertex must have one to four positive deform-bone influences; weights are normalized, zero-sum vertices fail, and a fifth nonzero influence is rejected rather than truncated.

Each v2 directory contains the canonical RGB and mask, radial Blender Z-pass EXR, face-ID EXR, skeleton, skin weights, topology, surface anchors, and `reference_camera_z.npy`. The NPY is float32 positive camera-space Z: foreground radial distance is divided by the intrinsics-derived camera-ray factor and every invalid/background pixel is NaN. `immutable_manifest.json` is mandatory and is written last; the loader verifies the byte size and SHA-256 of `fitting_bundle.json` and every artifact, and rejects missing, unexpected, duplicate, escaping or metadata-disagreeing paths.

### Semantic LTX limb reference

The pinned [Horse_2 profile](data/semantic_ltx_profiles/horse_2.v1.json), validated by [its JSON Schema](schemas/semantic-ltx-profile.v1.schema.json), explicitly maps the four anatomical source groups `fore_left`, `fore_right`, `hind_left`, and `hind_right` to exact deform-bone allowlists. No bone-name guessing or manual texture paint is allowed. Normalized skin weights classify polygon faces; candidate limb faces below the configured dominance gate fail the render. Skin-weighted median positive camera-Z then fixes each anatomical pair as near/far in the canonical camera and records that assignment in metadata.

`--rig-type HORSE_2` automatically selects the bundled profile; another exact profile can be supplied with `--semantic-profile`. V3 adds immutable `reference_ltx_semantic.png`: four high-contrast flat limb bands over a neutral readable body, composited over the unchanged canonical RGB ground/background. It is rendered after canonical RGB and before face-ID material override. Material slots, polygon indices, visibility, compositor and REST state are restored and asserted before continuing. All four labels must pass absolute and mask-fraction pixel gates, the transparent animal silhouette must exactly equal `reference_mask.png`, and resolution/camera/RGB/mask/profile hashes are persisted and revalidated by the loader.

## Runtime

From `R:\autorig\autorig-online\tools`:

```powershell
python -m pip install -r animation_fitting\requirements.txt
python -m animation_fitting doctor `
  --ffmpeg C:\path\to\ffmpeg.exe `
  --ffprobe C:\path\to\ffprobe.exe
```

`doctor` exits with code 2 when a required component is absent. Tracker inference is deliberately not mocked. To inspect the exact official checkout command and adapter boundary for one supported tracker:

```powershell
python -m animation_fitting doctor --tracker tap
python -m animation_fitting doctor --tracker cotracker
```

CoTracker is only an interchange adapter here. The official repository says most of that project is CC-BY-NC; legal/product approval is required before making it a production dependency. TAPNet is the cleaner initial implementation candidate for a commercial pipeline, subject to the model/checkpoint terms used in the actual deployment.

## Frame extraction

```powershell
python -m animation_fitting extract-frames `
  --video C:\fitting\horse_walk.mp4 `
  --output-dir C:\fitting\horse_walk_frames `
  --ffmpeg C:\tools\ffmpeg.exe `
  --ffprobe C:\tools\ffprobe.exe
```

The command writes contiguous `frame_000000.png` files and `frames_manifest.json`, including the source SHA-256, exact executable paths and extraction command. Existing frames are rejected unless `--overwrite` is explicit. `--fps` performs an explicit resample; without it, the source cadence is preserved.

## Tracker interchange

Point-to-rig mapping is explicit:

```json
{
  "schema": "autorig-tracker-anchor-map.v1",
  "tracks": [
    {"track_id": "front_left_hoof", "anchor_id": "HorseFrontLeg.L:1842"}
  ]
}
```

CoTracker-style JSON requires `tracks`, `visibility`, optional `confidence`, optional `track_ids`, and optional `query_frames`. TAP-style JSON substitutes `occluded` for `visibility`. The array layout is never guessed:

```powershell
python -m animation_fitting adapt-tracks `
  --adapter tap `
  --input C:\fitting\tap_tracks.json `
  --anchor-map C:\fitting\horse_anchor_map.json `
  --layout N,T,2 `
  --width 1280 --height 720 --fps 24 `
  --output C:\fitting\horse_observations.json
```

Boolean visibility/occlusion is accepted directly. Numeric values are rejected unless `--visibility-threshold` is supplied; that threshold is recorded in provenance. Missing confidence stays missing and has neutral weight 1.0 during optimization. It is never synthesized.

Custom importers can register a Python adapter with `register_tracker_adapter(name, callable)` in `observations.py`; the callable must emit `autorig-fitting-observations.v1` and pass `load_observations` validation.

## Canonical observations

The full contract is [schemas/observations.v1.schema.json](schemas/observations.v1.schema.json). Coordinates are zero-based pixels in the exact fitting-camera resolution. Frames are zero-based.

- Silhouette paths point to same-resolution images. Every nonzero pixel is foreground; no mask threshold is guessed. The optimizer measures the outside distance of tracked rig anchors.
- Depth paths point to same-resolution scalar `.npy` arrays or image files. `camera_z` means positive metric camera depth already. Relative depth is accepted only as `affine_to_camera_z` with explicit `scale` and `offset`; the engine does not estimate depth scale.
- Contacts contain an anchor ID and explicit frame numbers. Ground height defaults to the actionless bundle plane unless the observation declares another measured height. No foot-contact detector is hidden in the optimizer.
- Multiple tracks may not silently target the same anchor. Select or merge them before fitting.

The bundle camera uses Blender's convention: points in front of the camera have negative camera-space Z; exported pixel Y points downward.

## Fit

Validate the source bundle first:

```powershell
python -m animation_fitting validate-bundle --bundle C:\fitting\horse_bundle
```

Run one of the two explicit temporal modes:

```powershell
python -m animation_fitting fit `
  --bundle C:\fitting\horse_bundle `
  --observations C:\fitting\horse_observations.json `
  --config C:\fitting\horse_fit_config.json `
  --loop `
  --output C:\fitting\horse_walk_fitted.json
```

Use `--one-shot` for a fall, death or another action that does not return to its first pose. `--loop` adds first/last pose and velocity closure residuals; it cannot repair an upstream video whose content is not actually cyclic.

By default, only axes covered by local `LIMIT_ROTATION` constraints are optimized. A rig without those constraints fails closed. `--allow-unbounded-joints` is an explicit diagnostic escape hatch; it exposes missing axes over the Euler principal interval and must not be treated as production anatomy.

The optimizer config contract is [schemas/fitting-config.v1.schema.json](schemas/fitting-config.v1.schema.json). Every objective weight, robust loss, variable cap and iteration cap is persisted in the result. An explicit `active_bones` list is useful for keeping a horse clip tractable, but roots, helper bones and non-deform bones are rejected.

SciPy termination is fail-closed: both `result.success` and a positive `result.status` are required. Exhausting `max_nfev` (status 0), any other unsuccessful termination, or non-finite parameters/cost raises `OptimizationError`; the CLI does not write the requested fitted-animation file.

## Output and QA

The output contract is [schemas/fitted-animation.v1.schema.json](schemas/fitted-animation.v1.schema.json). Each frame includes:

- root translation and rotation vector;
- every bone's local 4x4 matrix, local translation and normalized XYZW quaternion;
- the original rig/observation hashes, config and optimizer termination fields.

QA contains only measured values: point visibility/survival, reprojection errors, behind-camera count, calibrated depth error, silhouette outside distance, contact height/slide, joint-limit violation, temporal jerk, loop seams and rigid bone-origin-distance error. `qa.decision` is always `null`; acceptance thresholds belong to the reviewed clip specification/admin approval flow, not this numerical engine.

## Blender motion authoring and game exports

`apply_fitted_motion.py` consumes `autorig-fitted-animation.v1` and runs only inside Blender background mode. Current compatibility contract is Blender 4.3 through 5.x; it handles both legacy 4.3 F-Curves and the layered Action/slot API used by 5.x.

The motion JSON now carries an explicit `autorig-fitted-transform-contract.v1`:

- root local matrices are in world space;
- child local matrices are relative to their parent bone;
- rotations are keyed as quaternions for every bone;
- translation is keyed only for the declared root bones unless `explicit_bones` is present;
- scale animation is forbidden.

The target is never auto-guessed. Supply exactly one exact armature name or, preferably, a SHA-pinned [motion target manifest](schemas/motion-target.v1.schema.json):

```json
{
  "schema": "autorig-motion-target.v1",
  "source_sha256": "<lowercase SHA-256 of canonical source>",
  "armature_name": "HorseRig",
  "armature_data_name": "HorseRigData",
  "bone_names": ["Root", "Spine", "Neck"],
  "bone_parents": {"Root": null, "Spine": "Root", "Neck": "Spine"}
}
```

Local Blender 4.3 example:

```powershell
& 'C:\Program Files\Blender Foundation\Blender 4.3\blender.exe' `
  --background --factory-startup `
  --python R:\autorig\autorig-online\tools\animation_fitting\apply_fitted_motion.py -- `
  --source C:\fitting\horse.blend `
  --motion C:\fitting\horse_walk_fitted.json `
  --semantic-action-id horse_walk_forward `
  --output-dir C:\fitting\authored `
  --fps 24 `
  --target-manifest C:\fitting\horse.motion-target.json
```

The applier validates every frame and every target bone, reconstructs pose-space matrices in hierarchy order, verifies the evaluated pose, mutes constraints in the derived asset, clears old Actions/NLA/drivers, and creates exactly one semantic Action. It rejects undeclared child translation, scale animation, more than four skin influences, generic action names, mismatched source hashes/hierarchies, output collisions and ambiguous armatures.

Writes first go to a private staging directory. After Blender, FBX and GLB validation succeeds, the manifest is promoted last. The canonical source SHA is checked again and the source is never saved. Successful stdout contains exactly the `AUTORIG_FITTED_MOTION=` marker; failure returns nonzero and prints `AUTORIG_FITTED_MOTION_ERROR=` without a success marker.

Artifacts are:

- `<semantic_action_id>.blend` with one named Action and no NLA tracks;
- `<semantic_action_id>.fbx` with one semantic animation take;
- `<semantic_action_id>.glb` with mesh, skin, exactly one named animation and no `JOINTS_1`/`WEIGHTS_1`;
- `<semantic_action_id>.animation-manifest.json` conforming to [the fitted asset bundle schema](schemas/fitted-asset-bundle.v1.schema.json), with SHA-256, bytes, FPS and measured GLB duration.

### Exact F1 Blender 5.1 invocation

Read-only inspection on 2026-07-13 confirmed F1 uses Blender `5.1.0` (build hash `adfe2921d5f3`) at `C:\Program Files\Blender Foundation\Blender 5.1\blender.exe` and its converter checkout is `C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\Vlado_Blender`. The same inspection confirmed Action layers/slots and every required FBX/glTF operator option. After a separate approved deployment places this script there, this local PowerShell command invokes it without shell quoting ambiguity:

```powershell
$remote = @'
& 'C:\Program Files\Blender Foundation\Blender 5.1\blender.exe' --background --factory-startup --python 'C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\Vlado_Blender\apply_fitted_motion.py' -- --source 'C:\fitting\horse.blend' --motion 'C:\fitting\horse_walk_fitted.json' --semantic-action-id horse_walk_forward --output-dir 'C:\fitting\authored' --fps 24 --target-manifest 'C:\fitting\horse.motion-target.json'
'@
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($remote))
ssh f1 "powershell.exe -NoProfile -EncodedCommand $encoded"
```

This implementation task did not copy files to F1 or modify the F1 converter repository.

Run the deterministic horse and contract tests:

```powershell
python -m pytest animation_fitting\tests -q
```

The suite includes pure contract tests, a real two-bone horse motion-authoring canary, an actionless-render canary poisoned with object/data Actions, NLA, drivers, constraints and a shape key, and a five-region semantic horse canary. The semantic canary verifies explicit fore/hind groups, camera-derived near/far, four pixel gates, exact mask equality, unchanged RGB outside the animal, material restoration, v3 loading, tamper rejection and manifest coverage. The actionless canary also verifies camera-Z and proves that a second armature, a geometry modifier and five deform influences fail without a publishable manifest. When `C:\Program Files\Blender Foundation\Blender 4.3\blender.exe` exists, the Blender canaries run automatically. Override the executable with `AUTORIG_BLENDER_43`; the semantic path is also manually checked on the local Blender 5.x runtime used for newer Horse_2 presets.

## Primary implementation references

- [Google DeepMind TAPNet/TAPNext repository](https://github.com/google-deepmind/tapnet) — official point-tracking implementation and interchange reference.
- [Meta CoTracker repository](https://github.com/facebookresearch/co-tracker) — official CoTracker implementation and license notice.
- [Meta Segment Anything 2 repository](https://github.com/facebookresearch/sam2) — official video segmentation predictor for producing mask observations.
- [Video Depth Anything repository](https://github.com/DepthAnything/Video-Depth-Anything) — official temporally consistent relative-depth implementation; its output still needs explicit camera-depth calibration here.
- [MoCapAnything, CVPR 2026 paper](https://openaccess.thecvf.com/content/CVPR2026/papers/Gong_MoCapAnything_Unified_3D_Motion_Capture_for_Arbitrary_Skeletons_from_Monocular_CVPR_2026_paper.pdf) — primary paper describing arbitrary-skeleton motion capture followed by inverse kinematics.
- [SciPy bounded robust least squares](https://scipy.github.io/devdocs/reference/generated/scipy.optimize.least_squares.html) — solver used by the temporal constrained objective.
- [FFmpeg formats documentation](https://ffmpeg.org/ffmpeg-formats.html) — primary `image2`/sequence format documentation.
- [Blender PoseBone API](https://docs.blender.org/api/current/bpy.types.PoseBone.html) — pose-space matrices and relative transform channels used by the bake.
- [Blender Action API](https://docs.blender.org/api/current/bpy.types.Action.html) — semantic Action data, layers and slots.
- [Blender glTF 2.0 exporter manual](https://docs.blender.org/manual/en/latest/addons/import_export/scene_gltf2.html) — Actions/NLA, animation and four-influence export behavior.
