# Changelog

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
