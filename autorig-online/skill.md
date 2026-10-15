# AutoRig.online Agent Skill

AutoRig.online is a cloud service that rigs and animates 3D models with the AutoRig V3 conveyor. One task takes any input (a 3D model, a picture, a picture of a car, or text) and returns a rigged, animated model in the V3 viewer, plus downloads for Unity, Unreal Engine and Blender. Use the public site for human workflows and the agent API for automated upload, task tracking and download flows.

## What V3 Does

- **One conveyor for any input.** A task starts from a 3D model (GLB, FBX, OBJ, or a ZIP with textures), a picture (picture to 3D model to rig), a picture of a car (the vehicle conveyor), or text. 1 task = 1 viewer: every task page is the Unity WebGL viewer with an AI agent chat.
- **Rig in seconds, animated at once.** The fast rig builds the skeleton and skin from the geometry in seconds and the animation clips are retargeted at once. Quality checks (joint placement, skin tears, limb collisions, numeric deformation QA) run in the background of the viewer. The target is a rigged, animated model within one minute from pickup.
- **Categories, each with its own skeleton template, fit rules, skin rules, clip library and QA:** biped / humanoid, quadruped (dogs, cats, bears, cows, pigs and similar), equine (horses, deer, zebras: scapula, elbow, mid-neck and leg profile joints, planted-hoof gaits walk / trot / gallop), bird (wings are rigged as wings, never as arms), serpent, monster, multilimb, hands (a single hand, gloves, FPS arms: per-finger joints and hand clips), primate, and vehicle (cars).
- **Vehicles are rigged too.** From one picture the conveyor segments the body, wheels, doors, hood, trunk, hatches, glass, lights and bumpers; each part is isolated and restored as a whole object, the bare shell is drawn without openables and glass, wheels are normalised to cylinder colliders, doors are hinged, and the interior (seat, wheel, pedals, gear stick) takes a rigged driver. The parts are reviewed in the viewer; paid 3D clouds are used only after an explicit user button, the free farm is the default.
- **Textures.** A model's own textures are kept. A model without textures gets PBR textures automatically (albedo, roughness, metallic, normal, emissive).
- **Animation library per category.** Clips come from generated reference videos per category (walk, trot, canter, gallop, idle, rear, look around, attack and more), extracted once, retargeted automatically onto every new rig with numeric QA per clip. Procedural clips are only the fallback. Custom clips can be added in the viewer.
- **Skeleton confirmation is offered, never required.** The viewer offers the skeleton for confirmation; the animation is made without waiting for it. Blueprint mode (key B) edits bones with symmetry, and the model is re-rigged 15 seconds after the last edit or by the button.
- **Viewer.** Unity WebGL. It opens on a black scene at once, plays the stages (voxels, erosion, bones, rig) while the mesh loads, then loads the chosen environment scene. One shared session agent per task, answering in the task creator's language.
- **Downloads.** GLB, FBX, USDZ, ZIP, Unity package and Blender file.
- **Quality gates.** Every bad model becomes a regression test and every engine release passes the autotests before it goes live. Rig quality comes before speed.
- **Languages.** Interface and agent answers in English (no URL prefix), Russian, Chinese, Hindi and Persian (right-to-left): `/ru/…`, `/zh/…`, `/hi/…`, `/fa/…`.

## Public Pages

- Home and upload (one upload zone for any input): https://autorig.online/
- How it works: https://autorig.online/how-it-works
- FAQ: https://autorig.online/faq
- Guides index: https://autorig.online/guides
- Gallery of rigged models: https://autorig.online/gallery
- Animal and creature rigging (quadruped, equine, bird, serpent, monster, multilimb): https://autorig.online/animal-rig
- Rigging in any pose: https://autorig.online/t-pose-rig
- Format pages: https://autorig.online/glb-auto-rig, https://autorig.online/fbx-auto-rig, https://autorig.online/obj-auto-rig
- Image to rigged character: https://autorig.online/image-to-rigged-3d-character
- Per-animal pages follow the pattern `https://autorig.online/{animal}-auto-rig` (dog, cat, horse, bear, cow, deer, elephant, giraffe, mouse, pig, rabbit, turtle).
- Blender plugin: https://autorig.online/blender-plugin
- Developer overview (API reference): https://autorig.online/developers
- Buy credits: https://autorig.online/buy-credits
- A finished task is public at `https://autorig.online/task?id={task_id}` (the V3 viewer).

## Agent API

- Register an agent with `POST /api/agents/register` (no Google sign-in; limited per client IP, 15 per hour by default). The response holds `api_key` and `agent_id`.
- Send the key on every request as `Authorization: Bearer <key>` or `X-API-Key: <key>`. Keys look like `ar_<prefix>_<secret>`. `GET /api/agents/me` shows the agent's free-tier balance; credit purchases are tied to Google sign-in on the website.
- Create a task with `POST /api/task/create` (JSON with `input_url`, or multipart with `input_url` or a file upload), poll `GET /api/task/{id}` until `status` is `done` or `error`, list files with `GET /api/task/{id}/cached-files`, then download.
- Stable download URLs: `GET /api/task/{id}/prepared.glb`, `/animations.glb`, `/animations.fbx`, `/model.glb`, and the full bundle `/bundle.zip` (see Task Download Bundles below).
- The result page is `https://autorig.online/task?id={task_id}`: the V3 viewer with the animation clips and the agent chat.
- Respect server rate limits (task creation is limited per minute) and retry only after transient failures.
- Full reference with curl examples: https://autorig.online/developers

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

- Rig worker dispatch sends the selected viewer backdrop as top-level `viewer_environment`, resolved from `task.viewer_settings.viewer_theme_selection.theme_id` to an absolute `/static/env/backdrops/viewer/...jpg?v=...` URL. Do not send this as root-level `background_url`; that field belongs to the worker's inpaint contract.
- Task media proxy URLs (`/api/video/{task_id}`, `/api/thumb/{task_id}`, `/thumb/{task_id}`) are mutable across restarts. Keep response cache revalidated and add a frontend media version (`guid`, `updated_at`, or task card `version`) so browsers do not reuse stale preview videos after regeneration.

## Supported Workflows

- Rigging and animating humanoid characters, quadrupeds, equines, birds, serpents, monsters, multilimb creatures, hands, primates and cars, in the categories listed above.
- GLB, FBX and OBJ inputs (also a ZIP with textures), pictures and text; exports to GLB, FBX, USDZ, ZIP, Unity package and Blender.
- Browser workflow through AutoRig.online (the V3 viewer with the agent chat) and a Blender-native workflow through the separate Blender plugin at https://autorig.online/blender-plugin.

## Discovery

- Sitemap index: https://autorig.online/sitemap.xml
- Marketing pages sitemap: https://autorig.online/sitemap/pages.xml
- LLM discovery file: https://autorig.online/llm.txt
- Standard LLM discovery file: https://autorig.online/llms.txt
- Public task SEO URLs stay on the canonical viewer URL shape: `https://autorig.online/task?id={task_id}`.
- Do not create, advertise, sitemap or canonicalize `/m` or `/m/...` pages. AutoRig task pages have one public URL shape only: `/task?id={task_id}`.
- Root `/sitemap.xml` links only SEO-gated task chunks under `/sitemap/gallery/part/{n}.xml`; those chunks include completed, non-adult tasks with poster/video and LLM-enriched metadata.
- The full public gallery task set without SEO gate is diagnostic-only at `/sitemap/gallery/all/part/{n}.xml`.
- IndexNow verification key is served from the site root and can be used by deploy/new-task jobs to submit changed public URLs.
