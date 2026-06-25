# AutoRig.online Agent Skill

AutoRig.online is a cloud service for automatic 3D model rigging and animation previews. Use the public site for human workflows and the agent API for automated upload, task tracking and download flows.

## Public Pages

- Home and upload: https://autorig.online/
- Gallery: https://autorig.online/gallery
- Animal and non-humanoid rigging: https://autorig.online/animal-rig
- Blender plugin: https://autorig.online/blender-plugin
- Developer overview: https://autorig.online/developers
- Buy credits: https://autorig.online/buy-credits

## Agent API

- Register an agent with `POST /api/agents/register`.
- Use the returned API key as a bearer token for authenticated agent requests.
- Upload supported 3D models through the task API, poll task status, then download generated outputs when complete.
- Respect server rate limits and retry only after transient failures.

## 3D Viewer Settings

- Public/global defaults: `GET /api/viewer-default-settings`.
- Full admin overwrite: `POST /api/admin/viewer-default-settings`.
- Admin global camera only: `POST /api/admin/viewer-default-camera`.
- Per-task owner/admin settings: `GET/POST /api/task/{task_id}/viewer-settings`.
- A camera saved through `/api/admin/viewer-default-camera` is stored with `global_camera_preset: true` and `bounds_policy: "ignore"`.
- Global camera presets are absolute Three.js camera transforms (`position`, `target`, `fov`, optional `up`) and intentionally bypass `model_bounds_signature` checks so the same admin-framed camera applies to all users/tasks.
- The task viewer still rejects a global camera locally when its distance/target are obviously incompatible with the loaded model bounds; in that case it falls back to `fitCameraToModelBounds()` instead of showing a tiny or lost model.
- When a global camera preset exists, task viewer settings reads merge that global `camera` over per-task settings; per-task lighting/theme data remains intact.

## 3D Viewer Runtime Contract

- Website preview is GLB-first for completed tasks: the frontend must try `GET /api/task/{task_id}/animations.glb` before any FBX preview path.
- `ready_urls` is not authoritative for `/animations.glb` availability; completed tasks can have a working endpoint even when `_all_animations.glb` is missing from `ready_urls`.
- FBX is a fallback/download/custom-clip source, not the default website model when the GLB endpoint works.
- If both animation GLB and FBX fail, the viewer should fall back to `prepared.glb` as a textured static preview instead of showing a broken/degraded animated model.
- Custom FBX animation previews are applied as clips to the current GLB runtime model. They must not replace the model.
- Custom FBX clips must be sanitized before playback: drop `.scale` tracks, drop non-root `.position` tracks, normalize root `.position` tracks relative to the first frame/current bind pose, and reject zero-compatible clips so the UI can fall back to a matching built-in clip.

## Task Download Bundles

- Full-task download uses the worker ZIP resolved as `/converter/glb/<guid>.zip` and streamed through `GET /api/task/{task_id}/bundle.zip`.
- `GET /api/task/{task_id}/cached-files` keeps the legacy direct-file `file_count`, but this is not the full bundle count.
- The authoritative full bundle count is returned as `bundle_file_count` only when `bundle_file_count_ready` is true.
- Backend reads worker sidecar metadata from `<guid>.zip.meta.json` and caches it under the task cache `.meta` directory.
- If worker metadata is missing while the ZIP is available, UI must show a generic `Download All Files` label instead of falling back to the smaller direct-file count.
- `fallback_cache` count is valid only when the worker ZIP is unavailable and the backend builds the ZIP from cached direct files.

## Backend Worker Routing

- The background worker loop must synchronize all `processing` task rows with worker terminal state before stalled-worker checks and before dispatching new queued tasks.
- This ordering prevents false `Worker stalled` alerts when a converter already reports a terminal failure but the backend task row has not yet been updated.
- Worker capacity uses both worker-reported active tasks and backend-assigned tasks; do not dispatch a new task to a worker just because its live `/server-status` is free while the backend still has an unsynchronized processing row.

## Supported Workflows

- Humanoid character auto-rigging.
- Animal and non-humanoid V2 rigging for models such as quadrupeds, creatures and spider robots.
- GLB, FBX and OBJ-oriented rigging flows.
- Browser workflow through AutoRig.online and Blender-native workflow through the separate Blender plugin.

## Discovery

- Sitemap index: https://autorig.online/sitemap.xml
- LLM discovery file: https://autorig.online/llm.txt
- Standard LLM discovery file: https://autorig.online/llms.txt
- Public task SEO URLs stay on the canonical viewer URL shape: `https://autorig.online/task?id={task_id}`.
- Root `/sitemap.xml` links only SEO-gated task chunks under `/sitemap/gallery/part/{n}.xml`; those chunks include completed, non-adult tasks with poster/video and LLM-enriched metadata.
- The full public gallery task set without SEO gate is diagnostic-only at `/sitemap/gallery/all/part/{n}.xml`.
- IndexNow verification key is served from the site root and can be used by deploy/new-task jobs to submit changed public URLs.
