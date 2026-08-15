# Changelog

## v0.02.005 - Storage-host monitoring and completion-email idempotency

- Added task-scoped completion-email claims plus Resend idempotency keys so concurrent progress requests cannot send duplicate ready emails.
- Added storage-host-specific disk-pressure and six-hour health-check units for ports 8200/8210 and `/srv/autorig` runtime paths.
- Made the stability checker configurable through `AUTORIG_HEALTHCHECK_*` without changing legacy VPS defaults.

## v0.02.004 - Storage-host migration and durable task artifacts

- Added a restart-safe artifact cache with verified 8 MiB range resume, worker-host pinning, atomic publication, ZIP CRC checks, and per-task SHA-256 manifests.
- Guaranteed a 24-hour full-artifact window and protected the last ZIP, GLB, FBX, viewer model, and poster while enforcing a 250 GB soft cap and 120 GB disk reserve.
- Added cache-first authorized downloads through nginx `X-Accel-Redirect`, preserving byte ranges and existing viewer/download URLs.
- Added read-only migration mode and isolated 8200/8210 systemd/nginx deployment files for the analytics storage host.
- Preserved production Renderfin fixes for worker-local Hunyuan downloads and stable Telegram result cards across restarts.
- Added a storage-host worker denylist so unhealthy farm nodes such as F7 cannot receive tasks even when an older database row still marks them enabled.

## v0.02.001 - Strict worker completion gate

- Read the additive worker completion-v2 fields from the task-status endpoint.
- Keep central tasks processing when URLs are ready but the worker has not returned both `status=Completed` and `finalized=true`.
- Treat early `/model-files` results as progress without replacing the original expected-output contract before finalization.
- Convert worker `Failed` or `finalization_errors` into a central task error while retaining the v1 fallback for historical tasks and workers.

## v0.01.009 - Canonical pre-convert metadata dispatch

- Recorded the production pre-convert poster metadata flow in the canonical repository.
- Added closed character category/subcategory metadata and forwarded it to converter workers.
- Kept preview/LLM failures non-fatal and bounded so metadata cannot block task dispatch.

## v0.01.008 - Task viewer GLB-first final preview

- Fixed done-task viewer boot so it always tries `/api/task/{id}/animations.glb` before `animations.fbx`.
- Stopped using `ready_urls` as proof that no viewer GLB exists; the backend can synthesize the preview GLB from worker files that are not public outputs.

## v0.01.007 - Animated viewer GLB source priority

- Prefer task viewer `<guid>_all_animations_threejs_preview.glb` over legacy `<guid>_all_animations.glb` when serving `/api/task/{id}/animations.glb`.
- Require both mesh and animation data for cached `animations.glb` responses so skeleton-only or static GLBs fall through to the existing FBX viewer fallback.
- Moved `animations.glb` cache entries to source-specific names to avoid serving stale pre-hotfix GLB cache files.

## v0.01.006 - Viewer animation cache recovery

- Rejected meshless `animations.glb` task assets so the viewer can fall back to animation FBX.
- Added cache-busted task viewer model URLs for `animations.glb`, `animations.fbx`, and `prepared.glb` to avoid stale invisible GLB browser cache.

## v0.01.005 - Orientation-safe worker dispatch

- Reused a single site orientation-to-worker transform helper for background rig dispatch.
- Preserved authoritative rig orientation through FBX pre-conversion continuation.
- Added authoritative orientation to admin bulk restart worker payloads.

## v0.01.004 - Default site rig orientation

- Defaulted browser-created rig tasks to an authoritative identity orientation when the user does not rotate the model manually.
- Preserved legacy worker orientation sweep for API payloads without `local_rotation_authoritative`.
- Ensured task restarts dispatch saved `rig_orientation` even when no separate model transform is present.

## v0.01.003 - Site-authoritative rig orientation

- Added manual face-direction controls to rig task creation and manual restart flows.
- Stored `rig_orientation` snapshots with authoritative `local_rotation` for site-created rig tasks.
- Passed authoritative orientation to converter workers so humanoid tasks can skip the worker orientation sweep.

## v0.01.002 - Worker viewer environment contract

- Added `viewer_environment` snapshots from selected viewer themes to task settings.
- Passed viewer environment snapshots to rig workers through worker payloads and `rig.json`.
- Preserved selected theme snapshots across task creation, viewer settings saves, auto-select, and restart flows.
