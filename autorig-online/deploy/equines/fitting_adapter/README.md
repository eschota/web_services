# MT quadruped to animation_fitting adapter

`prepare_fitting_adapter.py` creates a source-hash-bound job for an existing
V3 `rigged.glb` and `rig.json`. It requires the exact four MT leg chains plus
Head and Tail, and never supplies synthetic joint limits.

1. Run `prepare`. The resulting JSON contains the exact Blender 4.3
   `render_actionless_bundle.py` arguments.
2. Produce the actionless bundle with the pinned September
   `animation_fitting` tree. `patch_blender5_exporter.py` creates a separately
   pinned copy which uses `render.use_compositing` on Blender 5 and
   `scene.use_nodes` on Blender 4. The original source is never changed.
3. Run `bind-anchors`. It verifies every immutable bundle member and the source
   GLB hash, then selects four actual sole-band surface vertices and actual
   head/tail surface vertices. The output is a tracker anchor map; the bundle
   itself remains immutable.

The adapter rejects missing bone metadata, hierarchy drift, source changes,
uncovered bundle files, a mismatched bundle source, and implausibly separated
hoof anchors. Video Depth Anything remains relative until it is calibrated to
the bundle's `reference_camera_z.npy`. Single-view turns are not accepted
without that calibration or synchronized multiview evidence.

`build_code_bundle.py` packages only the patched exporter, its Python/JSON
contracts and this adapter. It contains no model weights, private assets or
runtime environment. Its ZIP-only package initializer exposes `errors` and
does not eagerly import optimizer/observations/rig or SciPy; the manifest pins
both the canonical initializer and this exporter-only subset. The canonical
source initializer is unchanged. `farm_actionless_job.ps1` verifies the ZIP hash, runs a
CPU-only zebra export into `<MT_ROOT>/.work`, enforces a 64 MiB bundle ceiling,
and binds the six actual surface anchors. `zebra_actionless_job.json` is a
prepared, undispatched job specification.

`joint_limit_candidate.py` is the next-phase declaration gate. It accepts only
an explicitly authored, source/rig-hash-bound, run-scoped candidate with exact
bone axes and ranged regularization options. It does not infer constraints and
cannot approve or promote a category default.

`build_runtime_bundle.py` packages the full Python runtime source, schemas,
semantic configurations and runtime lock without models or customer data.
`farm_fit_video.py` is preflight-only by default. `--execute` invokes the
pinned tracking runtime and the existing optimizer CLI while retaining every
observation/report; it stops at pending QA. `--finalize` requires a separate
SHA-bound PASS receipt but currently writes `registry-export-pending.json`:
Blender-space local channels cannot be called registry34/glTF +Y until the
fitted source is applied and exported through Blender, then re-read in glTF
bases. It never writes the canonical library or sends DEV media. See
`farm_fit_job.example.json` for every required public option and proof.

`build_motion_export_bundle.py` creates the minimal Blender 4.3 subset for the
existing `apply_fitted_motion.py`. `farm_export_fitted.py` is preflight-only by
default; `--execute` applies the fitted source to a derived copy, verifies the
original GLB hash stayed unchanged, exports a one-action GLB, then parses the
actual glTF accessors. Its result contains the actual glTF +Y reference
skeleton and node-local animation channels and remains
`candidate_pending_real_qa` with promotion disabled. Invocation fields are in
`farm_export_job.example.json`.
