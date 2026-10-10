# V3-only production cutover

Owner instruction, 2026-10-10: replace the entire new-task conveyor and converter
fleet with V3. Replace only the task page with the current Unity viewer, agent,
rig/animation inspection and effects. Do not redesign other site pages.

## Verified starting state

Production inspected 2026-10-10: release `/srv/autorig/releases/adminfwd-20261009`;
storage PID 511626, Renderfin PID 693215, MT PID 2364036. MT contract is the
interactive `/kit` API; `GET /api/mt/v3` returned 404. The backend release has
`v3_viewer_routes.py` but no `v3_pipeline_adapter.py`. Local V3 dispatch/binding
modules therefore must not be described as production integration.

`tasks.create_conversion_task` accepts only rig/convert/generate; dispatcher
`start_task_on_worker` still calls the old worker path. Supported converter target
set from configuration/ownership rules: F1/F2/F7/F11/F13; verify actual serving
builds before rollout. F5 development and owner4090 are not production targets.

The private V3 worker currently wraps fastrig plus numerical QA; it is not proof
that GPU erosion and independent garment/hair processing run across the fleet.
Five-model full anatomical/clothing acceptance remains 0/5. Failed QA must be
reported as needs_review, not disguised as successful migration.

## Execution order and acceptance

1. Integrate source normalization and parallel source-bound hierarchy/Vision
   analysis into a durable V3 dispatch service. Inputs include GLB/FBX/OBJ and
   image/video generation; output of generation re-enters the same conveyor.
   Persist original task UUID, source hash, intent, revision, attempt and run ID.
2. Package the actual V3 producer for the authoritative converter source.
   Stages: source, analysis, rig, retarget, QA, publish. Retain partial previews,
   clothing/hair layers and QA failures. Never fall back to legacy rig implicitly.
3. Wire website, Telegram, API, generation, retry and convert to this dispatcher;
   retries retain attempt identity, callbacks/outbox must be idempotent. Preserve
   existing user sources and completed artifacts. Track version separately from
   user intent instead of silently changing convert/generate to rig.
4. Replace task.html content with only the current Unity task viewer. Resolve run
   server-side through the task binding, not an arbitrary user-supplied run ID.
   Authorize private tasks, agent sessions and exports; show real queue/stage
   progress before a model exists. Model/rig first, optional scenes/media last.
   Keep task URL and server-rendered social metadata. Do not modify gallery/home.
5. GLB/FBX exports for Unity/Unreal and a real Unity character package with
   controller/NPC behavior require the $20/month unlimited subscription.
   Interactive 3D preview remains free, per explicit owner decision; browser
   preview geometry cannot technically be made impossible to save.
6. Deploy an exact hashed converter artifact to F1/F2/F7/F11/F13 under canonical
   maintenance/deploy rules. Verify listener-owned serving build on EACH node.
   Archive rollback artifacts; do not delete old customer tasks or outputs.
7. Check persisted queue and AUTORIG_WIPE_QUEUE_ON_START before backend restart.
   Enable V3-only creation only with complete route/worker/export/callback receipts.
   Observe actual real tasks and show their resulting task viewer. Missing stages
   or failed QA remain explicit; source unit tests are not end-to-end proof.

## Current implementation tranche

`backend/v3_cutover.py` is a side-effect-free controller precondition and explicit
V3 intent/version policy, with focused tests. It is not wired or activated yet.
No fleet switch, service restart or customer record mutation was performed in
this tranche. Next implementation is the durable source/dispatch integration,
not a cosmetic task iframe without a real task-to-run binding.

Prepared and root-tested source slices: `v3_dispatch_outbox.py` persists v2
attempts/leases and requires independently fetched artifact bytes/hashes before
local done; `task_v3_shell.py`, `task-v3.html`, `task-v3-shell.js` resolve only
authorized persisted task/source/publication bindings. Raw MT done without
source-bound QA is needs_review. Combined Python tests: 28 + 18 subtests passed;
JS shell tests: 2 passed. These source slices are NOT mounted/activated.

Read-only fleet observation 08:44 UTC: F1/F7/F13/F2 serving processes respond but
have no V3 dispatch endpoint (404); serving commits are mixed. F11 has no port
7000 listener and cannot be counted as a V3 converter. No deployment was invoked.
Production storage has AUTORIG_WIPE_QUEUE_ON_START=1; render_tasks had only
Done/Error in the snapshot, but pending chargen and creation races must be checked
immediately before a restart. A historical empty snapshot is not restart safety.
