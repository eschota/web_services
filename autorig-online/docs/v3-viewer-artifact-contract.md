# AutoRig V3 viewer artifact contract

Status: experimental, read-only. V3 is not the production default.

The converter publishes one immutable directory per task under the runtime
root configured by `AUTORIG_V3_ARTIFACT_ROOT` (production default:
`/srv/autorig/data/v3`). The web service never accepts a disk path from the
browser and never scans outside the exact UUID directory.

```text
/srv/autorig/data/v3/<task-id>/
  manifest.json
  surface.json
  solid.json
  thin.json
  skeleton.json
  weights.json       # only after S2/S3 passes
  skinned.glb        # only after bind/export passes
```

`manifest.json` uses schema `autorig.v3.viewer-manifest/1`. Its `task_id` must
match the directory. `build.source_sha256` is the exact prepared GLB input;
`build.manifest_input_sha256` binds the complete converter input receipt. Both
are lowercase SHA-256. A stage is `complete` only when it has a lowercase
SHA-256 `receipt_sha256`. Allowed states are `complete`, `failed`, `pending`,
and `unavailable`.

Stage names, in order:

1. `source`
2. `c1_surface`
3. `c2_solid`
4. `c3_thin`
5. `c4_graph`
6. `r1_bones` (actual fitted anatomical hierarchy; C4 is not bones)
7. `s1_owner`
8. `s2_weights`
9. `s3_skin`
10. `final_deformation`

Artifact entries require `name`, `type`, local `file`, `sha256`, and `bytes`.
The API permits at most 16 artifacts, 64 MiB each, 128 MiB total, and only `.json`, `.bin`, or
`.glb`. Types are `voxel_points`, `skeleton_graph`, `fitted_bones`, `skin_weights`,
`skinned_model`, and `deformation_clip`. The server verifies size and SHA-256
before returning an artifact and exposes it only by manifest name.
Every artifact also declares its exact `stage`; its returned
`stage_receipt_sha256` comes from that complete stage. Type/stage bindings are
strict: voxel points only C1/C2/C3, geometry graph only C4, fitted bones only
R1, weights only S2, skinned GLB only S3, and deformation GLB only final.
All manifest and artifact responses are `private, no-store` so a later
visibility revocation cannot leave a shared-cache copy.

Voxel JSON uses schema `autorig.v3.voxels/1`:

```json
{
  "schema": "autorig.v3.voxels/1",
  "task_id": "uuid",
  "source_sha256": "64hex",
  "stage_receipt_sha256": "64hex",
  "coordinate_space": "model_local_gltf",
  "matrix_to_model": [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1],
  "point_count": 1,
  "positions": [0.0, 0.0, 0.0],
  "classes": [0],
  "voxel_size": 0.012
}
```

Positions are flat xyz triples. `classes` is optional and has one value per
point. `voxel_size` is required in the same coordinate units. Up to 50,000
voxels render as actual instanced cubes; larger sets render as an explicitly
labelled point cloud using the real voxel size. The viewer caps a payload at
250,000 points. If data is already in glTF model-local Y-up space, set
`coordinate_space` to `model_local_gltf` and omit `matrix_to_model` or use the
identity. Raw Blender world data must instead declare
`coordinate_space: blender_world_z_up` and carry the exact column-major transform
derived from the imported GLB/object transforms. An assumed axis swap is not
acceptable.

Skeleton JSON uses schema `autorig.v3.skeleton/1`, the same task/source/stage
provenance and coordinate rules, plus exact integer `joint_count`, exact integer
`edge_count`, `joints: [[x,y,z], ...]` and `edges: [[joint_a,joint_b], ...]`.

Routes:

- `/viewer` — selector and task entry.
- `/viewer/<task-id>` — V3 viewer.
- `/task/<task-id>/viewer` — equivalent task-scoped route.
- `/<task-id>/viewer` — short task-scoped alias requested by the owner.
- `/api/v3/viewer/examples` — public tasks with valid published manifests.
- `/api/v3/task/<task-id>/manifest` — access-gated manifest; a legacy task
  receives only `source=complete`, `hash_bound=false`, and every V3 stage as
  `unavailable`.
- `/api/v3/task/<task-id>/artifact/<manifest-name>` — access-gated,
  allowlisted artifact bytes.

Never copy private F5 research artifacts into the production root merely to
make a demo. The task ID, prepared GLB hash, coordinate transform, stage receipt
and artifact hash must all correspond to the same real run.

