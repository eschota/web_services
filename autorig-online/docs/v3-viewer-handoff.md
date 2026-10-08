# V3 pipeline viewer handoff

Updated: 2026-10-09 Asia/Novosibirsk. This is a scoped handoff fragment for the
root `handoff.md`; it does not replace that canonical project checkpoint.

## Implemented locally, not deployed

- Backend: `backend/v3_viewer_routes.py`.
- Hook: a small `build_v3_viewer_router(...)` block immediately before the
  `/static` mount in the already-dirty `backend/main.py`.
- UI: `static/viewer-v3.html`, `static/css/viewer-v3.css`,
  `static/js/viewer-v3.js`.
- Tests: `backend/tests/test_v3_viewer_routes.py`.
- Frontend contract tests: `static/js/tests/viewer-v3-contract.test.mjs`.
- Evidence publication builder: `tools/v3_viewer/build_lab_publication.py`.
- Contract: `docs/v3-viewer-artifact-contract.md`.

The viewer loads the existing prepared GLB, renders bounded V3 voxel-point and
skeleton overlays, exposes stage timings/counts/receipts, colors real GLB skin
weights when present, and can play an actual animation/deformation only after
`final_deformation` is complete and the verified model has a real skin and
embedded clip. It has deterministic auto-rotation for evidence video.
Missing V3 outputs are displayed as unavailable; legacy skeletons or private
research outputs are not relabeled as V3.

Privacy/security: task lookup checks `tasks.is_public`, administrator, logged-in
owner, or matching anonymous owner. Artifact paths are UUID-rooted and exact
manifest allowlists; names, types, extensions, count, bytes and hashes are
bounded. Artifact responses use private cache semantics. The examples endpoint
lists only public tasks with a fully valid manifest.

## Verified

- `python -m py_compile backend/v3_viewer_routes.py`: pass.
- `node --check static/js/viewer-v3.js`: pass; frontend contract 13/13 pass,
  including strict overlay receipts/counts, exact model-byte SHA-256 and
  rejection of external GLB resources, invalid skin weights/joint indices and
  static or non-bone animation tracks.
- Production Python environment, isolated audit copy under
  `/srv/autorig/audits/animal-gpu-development/v3-viewer-test`: 4/4 manifest
  viewer validator/access tests 17/17 plus visibility schema tests 3/3 pass
  (valid hash-bound publication, changed
  bytes rejection, strict stage/type/receipt/counts, aggregate caps, traversal,
  symlink rejection, registered/anon/API-key ownership, admin, unrelated
  private denial, public anonymous access and visibility revocation).
- No backend/static release, service restart, DB write, customer submission, or
  converter default change has been performed by this work.

## Real candidates, read-only live DB observation

- Cat: `0ad64748-c493-4f25-a400-42425748ff71`.
- Horse: `7124a516-9bca-4006-ae39-2263904be8bd`.
- Dog: `50a9139d-3016-47e5-8734-bca71b4abd54`.

All were `done`, public, real animal tasks with a prepared GLB at the stable
`/api/task/<id>/prepared.glb` route when selected. They are candidates, not yet
V3-complete examples. Publish only exact per-task C1/C2/C3/C4 artifacts whose
source hash and coordinate transform match that prepared GLB. S2/S3/final stay
unavailable until the separate GPU bind gates pass.

F5 evidence has now been converted into a fresh local review publication at
`R:\autorig\.work\animal-gpu-skeleton-20261008\v3-preview\publication-v1`.
Publication receipt SHA-256:
`246ef43d6c50550424d2a3fb5f538ef1e968414a07592a1afed944ead238b483`.
The exact directories passed the production Python validator: cat and dog have
C1/C2/C3 plus C4 geometry graphs; horse has C1/C2/C3 and an explicit C4
`failed: not_one_connected_acyclic_component`. None claims anatomical bones,
weights, skin binding or final deformation.

## Next executable steps

1. Review the five new files plus the exact `main.py` hook while preserving all
   unrelated WIP.
2. Produce exact hash-bound C1/C2/C3/C4 overlays for the three candidates on
   isolated F5, including derived Blender-world-to-glTF-local transforms.
3. Stage an immutable VPS release with only reviewed V3 files and the exact
   hook. Create `/srv/autorig/data/v3` without changing DB state.
4. Run backend tests in the staged release, switch release, restart only
   `autorig-storage.service`, verify service PID/cwd and `/viewer`, manifest,
   privacy, artifact hash, and three public task pages.
5. Capture deterministic rotating/layer-toggle H.264 videos from the actual
   production viewer and send through the existing `/dev` Telegram validator
   as media. Caption task ID, build/source/manifest hashes, complete stages and
   known missing stages. Visual review is evidence, never an automatic gate.

