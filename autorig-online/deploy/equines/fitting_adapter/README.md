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
cannot approve or promote a category default. Use
`python joint_limit_candidate.py --options-get` for the exact current
`FittingConfig` field names, defaults and public ranges. Legacy
`joint_limit_weight` / `temporal_weight` / `jerk_weight` profiles are rejected.

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

When `existing_observations` and `existing_observation_manifest` are supplied,
preflight verifies every observation artifact, accepted diagnostics, exact
video/original-bundle identity, calibrated camera-Z evidence and all required
anchors. `--execute` then skips tracking, records an explicit binding from the
original calibrated bundle to the private joint-limit-only bundle, and fits
the existing observations. Joint-profile optimizer overrides must use exact
`FittingConfig` field names; legacy generic regularization names fail instead
of being silently ignored. `allow_unbounded_joints` always remains false.

`build_motion_export_bundle.py` creates the minimal Blender 4.3 subset for the
existing `apply_fitted_motion.py`. `farm_export_fitted.py` is preflight-only by
default; `--execute` applies the fitted source to a derived copy, verifies the
original GLB hash stayed unchanged, exports a one-action GLB, then parses the
actual glTF accessors. Its result contains the actual glTF +Y reference
skeleton and node-local animation channels and remains
`candidate_pending_real_qa` with promotion disabled. Invocation fields are in
`farm_export_job.example.json`.

`equine_constraint_fit.py` is the first single-Walk CPU fitter for measured
`equine_limb_constraints` output. It selects hinge axes from each immutable
source bone basis and the measured sagittal-plane normal, preserves rest bone
lengths/hierarchy through `RigBundle.forward_kinematics`, and writes a fresh
`autorig-fitted-animation.v1` compatible with `apply_fitted_motion.py`.
Engineering search bounds are public options, not anatomical defaults. Missing
frames are time-interpolated only and labelled `INFERRED_PENDING`; no opposite
leg or half-cycle mirroring occurs. Output remains unaccepted and requires real
post-retarget 3D QA. See `equine_constraint_fit_job.example.json`.
When source-bound torso constraints are present, the fitter may translate the
root only along the measured source forward/up axes; root rotation stays zero.
Without torso evidence the root remains exactly fixed as before. Hoof and torso
REST/fitted residuals and bound hits are recorded per measured frame. Progress
is emitted every ten frames and `max_fit_seconds` bounds the CPU solve.
Every completed window writes a non-accepted diagnostic checkpoint. A failed
TRF window preserves finite best parameters/cost/status/optimality. Explicit
`resume_from` is allowed only when objective schema, variable order, all source
pins and objective options match; solver-only tolerances/evaluation limits may
change and are recorded. Partial prefixes are never clips and never resume
automatically.
For a pinned failed window, `diagnostic_audit` evaluates the recorded best
finite parameter vector without optimization. It compares finite-difference
callbacks against declared exact sparsity and reports unexpected derivatives
plus cost split by hoof/joint/torso/ground/contact/temporal residual kind. It
creates no animation clip and requires a fresh diagnostic output.
Solver termination changes belong in a separate run-scoped
`autorig.equine-solver-control-profile/1` pinned to the immutable objective
profile SHA. It may override only solver controls; proportions, physics weights
and source constraints remain bound to the original objective profile.

`farm_constraint_fit_pipeline.py` is the isolated CPU farm entrypoint for real
pinned `tracks.json`/NPZ and the immutable bundle's `surface_anchors.json`. It records an ffprobe timing receipt, converts RAW2D
without filling or mirroring, builds source-plane constraints, writes a
run-only unaccepted profile and exact non-loop Walk contract, then executes the
bounded IK fitter. All artifacts are new under a fresh project work root; it
does not enqueue work, use a GPU, mount customer paths, publish or send media.

`export_completed_prefix.py` replays only a strictly verified contiguous
checkpoint prefix through immutable RigBundle FK. It performs no optimization
and emits `DIAGNOSTIC_PARTIAL_REFERENCE_NOT_LIBRARY`, loop=false, with exact
original frame mapping and original action length/hash. Remaining frames stay
explicitly pending; the partial reference cannot activate a library or hide the
failed suffix. See `export_completed_prefix_job.example.json`.

Source eligibility rejects impossible weighted-anchor reach and unsupported
own-chain targets before fitting; rejected original points remain diagnostic.
Objective v4 also measures same-frame limb-to-torso vertical displacement using
eligible own Toe/Foot/Leg/UpLeg and real measured torso anchors. Root translation
cancels from this residual. `limb_torso_vertical_weight` is a run-profile option;
no category default, synthetic contact, gait phase or mirrored leg is introduced.
The objective schema changes, so earlier completed prefixes cannot resume into it.
