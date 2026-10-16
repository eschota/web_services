# Picture → V3 at once: the viewer opens with the picture, the picture is isolated, the model is generated — spec (lead: Fable, 2026-10-16)

Owner, 2026-10-16, after uploading a Santa picture on the index page (task `77cc1b88`): «я залил картинку, а тут очень
долгое ожидание, и ничего не происходит, вьювер V3 сразу должен открываться с пустой сценой и показывать картинку в
ней, изолировать её и отправлять на 3д генерацию».

## What happens today (prod, read 2026-10-16 12:45 UTC)

* Index page (`static/js/app.js`, `startBatchGeneration`) → `POST /api/generate/from-image` (`backend/generation_api.py`):
  the task row is created at once with `pipeline_kind="generate"`, meta `stage=detect`, then the page redirects to
  `/task?id=…`. That part is quick.
* `backend/generation_tasks.py`, `_start_generation` (run on every pump tick): Vision detection first
  (`content_moderation.detect_character_for_generation`, farm / free router; 69 s on this task), **then** the renderfin
  job (Hunyuan 3D). While detection runs the row has no stage, no viewer: `/api/task/<id>/v3-view` answers
  `pipeline: "classic", stage_key: null, viewer: null, model: pending, progress 0.06` (clock based). The page shows a
  bare «Processing» for minutes.
* The verdict decides `riggable`: Santa with «arms hang down by the sides» → `pose: other`, `riggable: False` → the task
  would have ended as a **bare mesh, never rigged** (`_advance_generation`: «finished as a model (not riggable)»). That
  contradicts V3-only («Any pose works» is printed on the index page; V3 rigs any pose: stabilization, the reference
  leg rule). The lead flipped this task's meta to `riggable: True` by hand at 12:52 UTC.
* `_note_v3_generation` writes the live V3 stage (`generation:hunyuan …`, progress 0.02–0.30) only on the V3 route
  (`route_enabled("generation")`, which is `all` since 2026-10-10, so it does apply), but the task page does not read it
  for `pipeline_kind="generate"`: the v3-view builder reports `pipeline: "classic"`.
* The viewer has no way to show a picture before a run exists; vehicles have `vehicle-scene.js` (picture + model), a
  character has nothing.

## Contract (all three parts work to this)

### `GET /api/task/<id>/v3-view` for a `generate` task (status created / processing, meta stage detect / isolate / model)

```json
{"pipeline": "v3", "status": "processing",
 "stage_key": "generation", "sub_key": "detect" | "isolate" | "waiting_3d_worker" | "hunyuan" | "turntable" | "ready",
 "stage_seconds": 12, "progress": 0.02..0.30,
 "viewer": {"kind": "picture", "page": "/api/mt/unity/test/index.html",
            "params": {"picture": "https://autorig.online/u/<token>/source.png",
                       "cutout": "https://autorig.online/u/<token>/cutout.png" | null,
                       "task": "<task id>"},
            "rigged": false, "revision": "gen:<renderfin job id or 'detect'>"},
 "model": {"state": "generating"}}
```

When the mesh exists and the V3 binding made a run, the answer becomes the ordinary `viewer: {kind: "mt-run", …}` and
the page swaps the iframe (a new `revision`).

### The viewer (`unity/test/index.html` + a new `picture-plate.js`, loaded like the other blob scripts)

* With `?picture=<url>` (and no `run`): the black scene stays, an HTML overlay shows the picture centred (max 62 % of
  the viewport height, 16:9 letterbox, a soft frame, a subtle breathing animation), caption in the page language
  (`agent_lang`: en / ru / zh / hi / fa) «Generating the 3D model from your picture…» with the sub-stage from the parent
  (the task page posts `{type: "autorig.v3.stage", stage_key, sub_key, progress}` to the iframe every poll; the overlay
  shows it under the picture) and the picture's own progress ring from `progress`.
* When `cutout` is given, the overlay polls it (HEAD every 3 s, up to 10 min) and, once 200, fades the cutout in over
  the picture (the background dimmed to 25 %): «Isolated». The session agent still works (agent_task is passed).
* The overlay is removed when the page swaps to the run viewer (new iframe src), so nothing else to do.

### Backend (`generation_tasks.py`, `generation_api.py`, the v3-view builder)

1. **Dispatch first, detect alongside.** `_start_generation`: after the uploader-marked vehicle check, start the
   renderfin job at once (`start_character_gen_from_image`), set `stage=model`, then run the Vision detection in the
   background (`asyncio.create_task`, recorded into the meta when it answers: subject, pose, animal_type,
   detect_reason, vehicle verdict). A vehicle verdict arriving later routes the task to the vehicle conveyor
   (`vehicle_intake.maybe_route`) and cancels / ignores the character job (renderfin cancel if the API has it, else
   the result is dropped). Detection never blocks the job and never costs the person a minute.
2. **Isolate.** New meta stage `isolate`, run right after the upload (before or in parallel with dispatch, it takes
   1–3 s): the fleet mask service (`deploy/vehicle/masks_service.py`, HTTP `/masks`, the URL list the MT unit uses as
   `VEHICLE_MASKS_URLS`; the backend reads the same list from the MT unit's env file or a shared config, health-checked,
   least busy first) with a text query `"character, person, animal, creature, figure, object"` (GroundingDINO + SAM2),
   fallback the `boxes` input with the central 90 % box; the largest mask → `cutout.png` (RGBA, the mask feathered
   2 px, cropped to the bbox + 4 % margin, max 1536 px) saved next to the source (`/u/<token>/cutout.png`, served by
   nginx). The cutout URL goes into the meta (`cutout_url`) and the v3-view. Hunyuan gets the **cutout** when it exists
   (its own rembg then has nothing to guess), the original otherwise. Failures are silent (meta `isolate_error`), the
   generation goes on with the original.
3. **Always rig.** `riggable` is always `True` for a generated character; the verdict only informs the category hints
   passed to the V3 binding (`subject`, `pose`, `animal_type`). Remove the «finished as a model (not riggable)» branch
   for new tasks (keep it only behind an explicit `AUTORIG_GENERATION_RIG_ALL=off`). The legacy `convert` fallback
   (classic converter) is dead: `route_enabled` is `all`; if the V3 binding fails, retry on the next tick (as now).
4. **v3-view.** For `pipeline_kind="generate"` rows the builder answers the contract above: `pipeline: "v3"`,
   `stage_key: "generation"`, `sub_key` from the meta stage / renderfin job stage (`_v3_generation_stage` already maps
   hunyuan / waiting_3d_worker / turntable / ready; add `isolate` and `detect`), `stage_seconds` since the stage began,
   `progress` 0.02–0.30, and `viewer.kind = "picture"` with `picture`, `cutout`, `task`. Find the builder (grep
   `autorig.task-page-v3/1` in the backend) and keep the ordinary V3 run answer untouched.
5. **Tests.** `backend/tests/test_generation_picture_flow.py`: v3-view contract for a generate row (stage keys, viewer
   picture, cutout null → url), riggable always true, detection failure does not block the dispatch (fake renderfin /
   vision), isolate failure keeps the original. Gate: `sudo /srv/autorig/autotests/gate backend --release …` PASS; the
   release recipe of AGENTS.md (stage beside current, copy static from current before the repoint, restart).
6. **Measure** on three real pictures (the owner's Santa `77cc1b88`'s source, a quadruped picture, a prop): seconds
   from upload to (a) the viewer showing the picture, (b) the cutout, (c) the Hunyuan job start, (d) the rig in the
   viewer. Target: (a) < 3 s, (b) < 8 s, (c) < 10 s; (d) is the farm's time.

### Task page shell (`static/js/task-v3-shell.js`, the lead)

`viewerUrl` accepts `kind: "picture"` (params picture, cutout, task → the Unity page URL with `picture`, `cutout`,
`agent_task`, `agent_lang`); the top status strip shows «Генерация модели · Hunyuan 3D · 120 s · 12 %»; every poll
posts `{type: "autorig.v3.stage", …}` into the iframe.

## Both

* Production is the truth: copy `generation_tasks.py`, `generation_api.py` and the v3-view module from
  `/srv/autorig/current/autorig-online/backend`, patch at anchors, keep the patch script in the scratchpad, mirror into
  `R:\autorig\autorig-online\backend` and commit. The viewer file goes live in `/srv/autorig/data/motion_transfer/unity/
  test/` (atomic write as user autorig) and into the MT repo template `unity/AutoRigViewer/Assets/WebGLTemplates/AutoRig/`
  (push to main through a clean worktree; the local checkout has other agents' uncommitted changes, never reset).
* No DEV posts from the agents; results into the report and the run / task directories. The lead posts one sheet.
