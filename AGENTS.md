# web_services Project Rules

This repository is the local working copy for:

```text
R:\autorig
```

Remote repository:

```text
https://github.com/eschota/web_services.git
```

The production VPS checkout uses the same repository over SSH:

```text
git@github.com:eschota/web_services.git
```

Production SSH access is configured in the user's `~/.ssh/config` as:

```bash
ssh autorig-vps
```

That alias reaches `autorig.online` itself (37.187.57.177, `way.qwertystock.com`,
SSH on port 22744, user `debian`, passwordless `sudo`). It used to point at
185.171.83.65; that box is still up but serves no web traffic, and is now
`autorig-vps-legacy`.

`autorig.online` is deployed as immutable releases, not as a checkout that is
pulled in place:

```text
/srv/autorig/current -> /srv/autorig/releases/<commit sha>
```

* backend: `autorig-storage.service`, uvicorn on `127.0.0.1:8200`, working
  directory `/srv/autorig/current/autorig-online/backend`
* static: nginx serves `/srv/autorig/current/autorig-online/static`
* secrets: `/srv/autorig/secrets/` (not in Git)

Deploy by staging a new directory under `/srv/autorig/releases`, verifying it,
then repointing `current` and restarting the service. Never `git pull` inside a
release. `/root` is the old VPS layout and does not apply to this host.

Use this `AGENTS.md` as the project rule source. Do not create or rely on
Cursor `.cursor/rules` instructions for this project.

## Первая Заповедь Авторига (the First Commandment)

The owner's main rule, 2026-10-10. It is a concept he wants to test in
practice:

> Реалтайм параллельная разработка в продакшене … всегда всё пишется в прод,
> без локального тестирования, только живое обновление сервисов и
> тестирование на проде.

What it means for every agent:

* Every change goes to production right away and is tested there with curl,
  logs, a browser and real tasks. No local servers, no local QA gate.
* The task page always runs the newest code. Its JS and CSS are served so that
  every new task opened on the site loads the version an agent just wrote. They
  revalidate on every load and are never served stale from a cache.
* Live file writes are atomic: write a temp file, then rename it. A
  half-written file is never served, and a file shared by hardlink with an old
  release is never modified in place.
* A release is still how a change reaches `current`. Staging one and
  repointing `current` takes seconds, and it keeps rollback. Deploy every small
  change at once.
* Mirror every live change into Git right away (commit and push), so
  production and Git never drift.
* All activity goes to the owner visually for validation. Owner, 2026-10-10:

  > всю деятельность надо визуально медиа сообщениями отправлять в этот чат
  > мне на валидацию

  * Send every visible result (a render, a screenshot, a short video of the
    live page or the viewer) as a media message to the DEV channel
    `https://autorig.online/dev`: `POST https://autorig.online/dev/api/send`.
    The send contract is the skill
    `https://autorig.online/dev/skills/telegram-developer-validator`.
  * The message names the agent and the project and carries a one-line
    caption.
  * His verdicts (✅ Принять / 🔁 Улучшить) come back through
    `GET /dev/api/inbox?agent=<name>` and are orders.
* Agents work in parallel, one agent per project entity: Astra, fleet, task
  page, intake/dispatch, converter, and so on. Re-read the production copy of a
  shared file just before patching it. Never overwrite another agent's change.
* These still apply:
  * Secrets stay out of Git, logs and prompts.
  * Customer sources and outputs are never deleted.
  * A restart that wipes queues (`autorig-storage`, renderfin) is batched,
    and in-flight work is checked first.
    `render_tasks` alone is not enough: the startup reset also cancels graph
    renders that are only running in renderfin (3 were lost on 2026-10-10).
    Ask renderfin what a restart would cancel, on the VPS:
    `curl -s -X POST 'http://127.0.0.1:8210/renderfin/api-render/reset?dry_run=1&spare_non_graph=1'`
    (`queued_int` and `running_int` should both be 0).

### How a live static edit is made (2026-10-10)

* One command per file, from the VPS:
  `sudo python3 /srv/autorig/tools/live_static.py put <rel> <file>`, where
  `<rel>` is a path under `autorig-online/static` (e.g. `js/task-v3-shell.js`).
  * It writes the overlay `/srv/autorig/live/static` atomically.
  * Under one lock it stages a hardlinked release beside `current`, gives
    every overlay file a fresh inode in it, repoints `current` and clears the
    overlay copies that `current` now carries.
  * The output names the `previous` release, which is the rollback.
  * `--no-promote` keeps a change in the overlay only; `ls`, `rm`, `promote`
    and `gc` manage the overlay.
  * Backend code is not live-editable: it needs a release and a restart.
* nginx serves `/static/` from the overlay first, then the release.
  * JS/CSS whose `?v=` is an exact 10-hex content stamp are immutable.
  * Every other JS/CSS is `no-cache` with ETag, so browsers revalidate it on
    each load.
* `/task` is rendered per request: its template and partials come from the
  live roots, and every `/static` JS/CSS reference gets its current content
  stamp. A task page HTML/JS/CSS change needs no backend restart.
* `GET https://autorig.online/api/task-page/live` shows the rollout mode and
  the current stamps of the task page assets.

## AutoRig Fleet Status Is One API Call

Owner rule, 2026-10-10:

> сделай флот авторига правилом, чтобы каждый агент работая с сессией знал
> текущий статус каждого агента флота, надо сделать чтобы это было обычным
> базовым запросом к апи. иначе мы слишком много тратим на это времени.

* Every agent gets the state of the whole fleet with one request:
  Astra, session agents, Claude and Codex sessions.
  * `GET https://autorig.online/api/fleet`: JSON, rebuilt in memory every few
    seconds, public, no secrets.
  * `GET https://autorig.online/api/fleet?format=text`: the same as a plain
    table, one line per box. It is the cheapest form for an LLM.
  * `GET https://autorig.online/api/fleet/box/<id>`: one box (`f1`, `f2`,
    `f7`, `f11`, `f13`, `f12`, `f15`, `raptor`, `worker-4090`, `f5`).
* What the answer carries:
  * `summary_object`:
    * `autorig_capable_array`: converters AutoRig can dispatch to right now;
    * `autorig_dispatch_enabled_array`, `healthy_full_converters_array`;
    * `hunyuan_ready_array`, `ai_ready_array`, `v3_ready_array`;
    * `v3_target_commit`, `low_disk_array`.
  * `queues_object`: AutoRig created/processing tasks and processing per
    box, done/error per box over 24 h, Renderfin pending/rendering, and the
    converters' own queues.
  * `vps_object`: free disk, `new_tasks_paused_bool`, the release and the
    state of the AutoRig units.
  * Each box in `boxes_array` has:
    * `state_string`: `idle`, `busy`, `degraded`, `blocked`, `offline` or
      `out_of_fleet`, with a one-line `summary_string`;
    * `roles_array`: `converter`, `render`, `hunyuan`, `ai-node`,
      `blender-worker`, `trainer`;
    * `busy_with_array` and `queue_depth_int`;
    * `dispatch_object`: the `worker_endpoints` flag, membership in
      `AUTORIG_DISABLED_WORKERS`, the backend route (internal tunnel or public
      gateway) and whether the public gateway is up;
    * `build_object`: build, commit, deployed artifact SHA-256, deploy
      protocol and drift;
    * `v3_object`: `ready` and `blocked_by`. A node is V3-ready when
      `deploy_commit` and `boot_build_id` equal the accepted commit,
      `feature_flags.normalized_source` is true and the V3 route answers.
      `deploy_farm.bat` ships per-base delta artifacts, so the artifact
      SHA-256 differs per node by design: compare commits, and each node's
      artifact only with its own rollout record;
    * `gpu_object`, `disks_array` (every drive; `work_drive` marks the drives
      AutoRig works on) and `quarantine_array` (`_retired_*` sizes);
    * `blockers_array`, `warnings_array`, `notes_array`, `last_seen_utc`;
    * `services_object`: converter, render, hunyuan, ai_node, lora_sync,
      blender_worker and fleet_agent detail.
* Where it comes from:
  * the converter registry `/srv/autorig/secrets/renderfin-hunyuan.json`
    (server-status through the VPS tunnels, tokens stay server side);
  * `worker_endpoints` and the live `AUTORIG_DISABLED_WORKERS` /
    `AUTORIG_WORKER_TRANSPORTS` of autorig-storage;
  * Renderfin, each ComfyUI, the MT Blender workers;
  * the box agents.
* The service is `autorig-fleet.service`, running
  `/srv/autorig/fleet/fleet_api.py` on `127.0.0.1:8255`.
  * It lives outside the release tree, so a parallel web release cannot drop
    it. nginx sends `location = /api/fleet` and `^~ /api/fleet/` to it.
  * Source: `autorig-online/deploy/fleet/`. Deploy by writing the file there
    atomically and restarting `autorig-fleet` only. That never touches
    autorig-storage, and the restarted service answers at once from its last
    snapshot on disk.
* The box agent is the scheduled task "AutoRig Fleet Agent".
  * It runs `C:\ProgramData\AutoRig\fleet-agent\fleet-agent.ps1` every
    2 minutes and is read-only.
  * It runs on f1, f2, f7, f11, f13, f12, f15 and Raptor, and on worker-4090
    as a user task under `%LOCALAPPDATA%`.
  * It posts drives, GPU (nvidia-smi), AutoRig scheduled tasks, listening
    ports and `_retired_*` sizes to `POST /api/fleet/report`.
  * It updates itself from `/api/fleet/agent.ps1`. Keys:
    `/srv/autorig/secrets/fleet-agent-keys.json`; render boxes reuse their
    LoRA sync key.
* The accepted V3 converter is recorded in
  `/srv/autorig/data/var/fleet/v3_target.json`: `commit`, plus one record
  per node under `nodes` (base commit, delta artifact, canaries). Update it
  by read-modify-write; the Converter agent and the fleet agent both edit it.
* A converter deploy drains one node at a time:
  1. `sudo python3 fleet_drain.py drain <box>` (in `deploy/fleet/`) turns off
     its `worker_endpoints` row, Hunyuan pool flag and LLM routing, all read
     live;
  2. wait until `fleet_drain.py status <box>` says idle;
  3. run `deploy_farm.bat HEAD -Nodes <F..>`;
  4. run the V3 canary driver and `rig_canary.py <box> <tunnel port>`
     (legacy only_rig, which also runs the lazy asset preflight);
  5. `fleet_drain.py restore <box>`.
* The fleet service posts to the DEV channel (UTF-8, from the VPS) when a
  work drive of an in-fleet box drops under 10 GB. It re-arms above 15 GB,
  repeats every 12 h while low and escalates under 2 GB. Facts no probe
  can see, such as a box powered off on purpose, go into
  `/srv/autorig/data/var/fleet/operator_notes.json`.
* Do not probe boxes one by one over SSH to learn their state. If something
  is missing from the API, add it to the API or to the box agent.
* AutoRig reaches the converters through the VPS tunnels.
  * The map is `AUTORIG_WORKER_TRANSPORTS` in
    `/srv/autorig/secrets/autorig-rig-worker-transport.env`.
  * Since about 2026-10-06 the public `converter-fX.freestock.online` gateway
    answers "Node tunnel is offline" for f1, f2, f11 and f13.
  * A converter listed in `AUTORIG_DISABLED_WORKERS` gets no AutoRig work,
    whatever `worker_endpoints` says.
  * Changing either variable needs an autorig-storage restart.
* Fleet members:
  * converters with LLM: f1, f2, f7, f11, f13;
  * render with LLM: f12, f15, Raptor;
  * worker-4090: the owner's PC, and its GPU is his first.
  * f5 is out of the fleet by the owner's order of 2026-10-08.

## Rig Within One Minute

Owner rule, 2026-10-10:

> теперь еще нужно держать скорость рига в пределах одной минуты

* **Budget.** A rig takes at most 60 s, measured from the moment a worker takes
  the task to the moment the rigged model is animated in the task viewer.
  * Every agent who touches the rig path checks a change against this budget.
  * A stage that breaks the budget is a bug, not a slow model.
* **Deliverables are separate.** Unity package, FBX/blend, ZIP, preview video
  and YouTube are produced after that point, in the background. They never
  hold the viewer back.
* **The viewer always shows progress.** From pickup on, the viewer shows the
  stage the worker is in. After the rig is ready, it can replay the recorded
  stages (voxels, erosion, bones) as a cached animation.
* **How to measure:**
  * classic converter: `<worker>/converter/glb/<guid>/logs/stage_timing.jsonl`,
    over the VPS tunnel (`AUTORIG_WORKER_TRANSPORTS`). `<guid>_progress.txt` is
    the short timeline.
  * V3 conveyor: `seconds` per phase in `runs/<run>/phases.json` and
    `analysis/fast.json`.
* **Found on 2026-10-10** (task 66ba97ba on f1, only_rig, 23 950 verts, a
  sword in hand). It took 7 min 56 s, with the rig ready at about 3.5 min:
  * OpenPose orientation sweep: 90 s. It always runs all 24 candidates (6 views
    × 4 rotations), each one a fresh `OpenPoseDemo.exe` with hand and face nets.
    The perfect candidate (score 235 of 235) came third. Fix: stop early on a
    confident candidate.
  * Three separate binds, each starting its own Blender:
    * prepare_tpose bind: 23 s;
    * autorig PSEUDO_VOXELS: 22 s, failed BindCheck with 445 hits (the sword is
      a separate shell);
    * VHDS retry: 44 s.
  * Blender 5.1 face aux render: `scene.node_tree` no longer exists, so face
    markers came out as 0.
  * Retarget 65 s, Unity 143 s, packaging 68 s, all before `done`.
  * The V3 conveyor rigs in 4–10 s (`fastrig`), but its analysis and QA phases
    have taken up to 400 s.

## V3 Concept: Astra and Session Agents

Owner rule, 2026-10-10:

> полноценный ИИ Агент Астра и локальные агенты сессий — основная концепция V3
> протокола сайта и сервисов autorig.online — с новым вьювером в основе каждого
> таска, 1 таск 1 вьювер — любые инпут форматы, АИ агент сессии сам должен
> выбирать по какому пути развивать сессию и что делать с моделью засчет
> инструментов. Список инструментов должен быть по адресу
> www.autorig.online/dev/tools

Every V3 decision for the site and its services follows from it:

* **Astra** is the one full AI agent of autorig.online and its services. The
  owner: «@autorigbot агент уже есть», so Astra grows out of the existing
  @autorigbot agent rather than a new bot.
* **Session agents** are local: one per task session, working inside that
  task's viewer.
* **1 task = 1 viewer.** Every task is built around the new viewer. No task
  page exists without it.
* **Any input format** enters the same task conveyor: meshes, images, video,
  text.
* **The session agent chooses the path.** It decides how the session develops
  and what to do with the model by calling tools, not by following a fixed
  pipeline.
* **Tools are listed at `https://autorig.online/dev/tools`.** That catalogue
  is the one list of the tools Astra and the session agents can call. A new
  tool is not finished until it appears there. As of 2026-10-10 the URL
  returns 404 and still has to be built.

## User Language

Owner rule, 2026-10-10:

> мне надо чтобы и Агент понимал по английски и отвечал по английски тем у кого
> язык английский, чтобы он это знал от апи сайта, и чтобы сайт был переведен
> на вот этот язык и локализован

* Every agent (Astra, support_ai, session agents) answers each user in that
  user's language: English to English speakers, Persian to Persian speakers,
  and so on. It reads the language from the site API and never guesses. If the
  user writes in another language, it answers in the language of the message.
  It never defaults to Russian.
* The language is an API field (`backend/user_language.py`, contract
  `GET https://autorig.online/api/language`):
  * `GET /auth/me`, `GET /api/me/language`: `language`, `language_code`;
  * `GET /api/task/{id}`: `owner_language`, `owner_language_code`;
  * `POST /api/support-chat/session`: `language`, stored on
    `support_chat_sessions.language`;
  * admins and agents on the host: `GET /api/language/resolve?task_id=` or
    `support_session_id=`, `email=`, `anon_id=`.
* `language.code` is the language to answer in (any ISO 639-1 code). Put
  `language.agent_instruction` into the prompt. `language.ui` is the site
  interface: en, ru, zh, hi or fa.

Owner rule, 2026-10-10:

> надо чтобы на родном языке из локали браузера пользователя, а в конкретном
> чате АИ должен привязываться к своему овнеру

* The language to answer in is the native language of the person's browser
  locale. The order:
  * `navigator.languages`, sent by the page;
  * the `/api/me/language` field, which is browser-first too;
  * `Accept-Language`;
  * the browser languages recorded for the account or visitor;
  * only then the language-menu choice and the language recorded on the task;
  * then `en`.
* The interface language never decides the reply language. That covers the
  language menu, the cookie `autorig_lang` and `/fa/` URLs: they set
  `language.ui` only. A Russian browser with the menu on Persian gets Russian
  answers.
* A chat binds to its owner, the person who opened it:
  * A viewer chat or a support chat takes that person's language when it opens
    and keeps it.
  * The run's own chat belongs to the task owner.
  * Another visitor's language never enters someone else's chat.
  * The task owner's language is never used for a different person.
  * If the person writes in another language, the agent answers in the
    language of the message.
* Persian (`fa`) is not Hebrew (`he`); both are right to left. Tests:
  `backend/tests/test_user_language.py` (EN, RU, FA, HE, DE browsers and chat
  binding).
* Every user-visible string on the site goes through i18n keys
  (`static/i18n/<lang>.json`, `data-i18n`, `I18n.t()`). Raw server text never
  reaches a user: errors carry `detail.error_string` and a localized
  `message_string`. Persian is right to left. Page URLs per language are
  `/fa/...` and `/ru/...`; English has no prefix.

## General Workflow

### Persistent development handoff

For ongoing AutoRig development, read the root `handoff.md` after context
compression, a new session, or an agent handoff, then follow its canonical
project checkpoint. Verify Git and live runtime facts before acting. Keep the
canonical handoff and its documented server mirrors current after material
verified progress, blockers, or changed next steps; verify mirror SHA-256.
This is an owner requirement from 2026-10-09. Never store secrets/private assets
in the handoff, and never treat an old snapshot as fresh operational evidence.

Use a production-first deployment workflow for AutoRig.online. Do not run local
dev servers, local preview servers, or local browser QA for this project unless
the user explicitly asks for a local-only experiment. The local checkout exists
for source control, commits, and exact file preparation; runtime verification
happens on production.

1. Make changes in `R:\autorig`.
2. Check the local working tree before editing:

```bash
git status --short
```

3. Keep changes scoped to the requested service directory.
4. Commit and push from the local repository when the change should be
   preserved.
5. Deploy immediately to production and test against `https://autorig.online`
   with `curl`, service logs, and browser checks.
6. Fix production-visible issues in the same deploy loop. If a direct production
   edit is needed to unblock the site, mirror the exact change back into
   `R:\autorig`, commit it, and push it.
7. Deploy `autorig.online` as a new release, never by pulling in place:

```bash
ssh autorig-vps
CUR=$(readlink -f /srv/autorig/current)
sudo cp -a "$CUR" "$CUR-<change>"          # stage beside the live release
# copy in the exact changed files, run the relevant tests inside the staging dir
sudo ln -sfn "$CUR-<change>" /srv/autorig/current.new
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
sudo systemctl restart autorig-storage.service
```

The deployed tree can be ahead of a local branch, so never ship a branch
wholesale to fix one thing: carry only the changed files, and patch a shared
file such as `main.py` at exact anchors. Rolling back is repointing `current`
at the previous release and restarting.

Avoid SSH-only code edits. If an emergency production edit is unavoidable, copy
the exact change back to `R:\autorig`, commit it, and push it so local git
and production do not drift.

Never commit runtime caches, browser state, virtualenvs, build artifacts,
secrets, logs, uploaded files, or generated backup files unless the task
explicitly requires it.

## Repository Layout

Important top-level service directories:

```text
autorig-online/          AutoRig.online production web app
autorig/                 AutoRig related assets/tools
qwerty_vpn/              QwertyStock VPN gateway/proxy service
CGTrader_SUBMIT_SERVER/  CGTrader submit server
```

There are also historical/runtime-looking directories such as `.config`,
`.local`, `.venv-autorig`, `.vscode-remote-containers`, `.wdm`, and `opt`.
Treat these as sensitive or runtime state unless the user gives a specific task
for them.

Virtualenv directories are runtime state, not source code. Keep them ignored:

```text
autorig-online/venv/
autorig-online/mcp/.venv/
qwerty_vpn/gateway/venv/
CGTrader_SUBMIT_SERVER/venv/
```

If a cleanup commit removes tracked venv files, do not deploy it with a plain
`git pull` over production unless the production venvs have first been copied
aside or recreated. Safe sequence: copy the venv directory outside the repo,
pull the cleanup commit, move the venv back into the same ignored path, then
verify the matching service. `autorig.service`, `autorig-telegram.service`,
`qwerty-gateway.service`, the AutoRig MCP process, and
`cgtrader_submit.service` have been observed using in-tree venv paths on the
VPS.

## AutoRig.online

Production application path (through the release symlink):

```text
/srv/autorig/current/autorig-online
```

Do not use `/opt/autorig-online` as production. Do not deploy from `/opt`.
There may also be a small `/autorig-online` directory at filesystem root; it is
not the current backend checkout.

Active production wiring:

- `autorig-storage.service`: AutoRig backend. There is no `autorig.service` on
  this host; that name belongs to the retired VPS.
- Backend working directory: `/srv/autorig/current/autorig-online/backend`,
  which follows the release symlink.
- Backend command:

```bash
/srv/autorig/venv/bin/python3 -m uvicorn main:app --host 127.0.0.1 --port 8200 --workers 1 --no-access-log
```

- Backend listens on `127.0.0.1:8200`; Renderfin listens on `127.0.0.1:8210`.
- `autorig-storage-telegram.service`: AutoRig Telegram bot.
- nginx active site: `/etc/nginx/sites-enabled/autorig.online-storage`.
- nginx static root: `/srv/autorig/current/autorig-online/static`.
- nginx proxies API/backend traffic to `http://127.0.0.1:8200`.
- Converter nodes are reached through local SSH tunnels on `127.0.0.1:15xxx`,
  listed with a per-node token in `/srv/autorig/secrets/renderfin-hunyuan.json`.

For AutoRig changes, touch only `autorig-online/...` unless the user asks for
cross-service work.

### YouTube channel integrations

- AutoRig is its own service and must keep its existing YouTube destination and
  background upload behavior. Its automatic uploader reads only the
  `youtube_credentials` row and `AUTORIG_YOUTUBE_CLIENT_ID/SECRET` (falling back
  to its original `GOOGLE_CLIENT_ID/SECRET`). Never authorize or save U3D tokens
  into this row, and never route AutoRig task uploads to U3D.
- U3D (`U3d Indie Game Developer`, `@unlim3d`, channel ID
  `UCpCN8wm6UXr8Ke_m-zSaThQ`) is a separate provider integration, only for
  agents the owner explicitly enables. It has a separate OAuth callback at
  `https://autorig.online/api/oauth/u3d-youtube/callback`, separate
  `u3d_youtube_credentials` storage, and `U3D_YOUTUBE_CLIENT_ID/SECRET`.
  Provision agent access only through the explicit `U3D_YOUTUBE_AGENT_KEYS`
  allowlist. Agents authenticate with `Authorization: Bearer <U3D_AGENT_KEY>`;
  this credential is independent from AutoRig admin API keys. The owner can
  copy it from `/dev/youtube` while signed in as an administrator. The stable
  machine-readable contract is `/dev/youtube/skill.md`.
- Both integrations use only the `youtube.upload` scope. Never put client
  secrets or refresh tokens in Git, browser storage, task output, or logs.
- `/dev` is the separate Telegram Developer Validator and must remain available.
  Link the independent U3D provider docs at `/dev/youtube`; Nginx proxies
  `POST /dev/api/youtube/videos` to the provider-only endpoint. It accepts a
  video file plus title, description, optional comma-separated tags, and
  `privacy_status`. Default visibility is `public`; report the actual response.
- YouTube classifies Shorts from the uploaded video's aspect ratio and length;
  the API has no Shorts flag. Check current YouTube rules before giving
  duration/format guidance. Long-form videos use the same resumable upload API.
- Stream multipart uploads from FastAPI's request-scoped `UploadFile` spool
  directly into the resumable YouTube upload, then close it. Do not make a
  second video copy or put uploads or OAuth credentials in the release tree.
- New or unverified YouTube Data API projects can be restricted to private
  uploads until YouTube completes its compliance audit. Never promise public
  publishing until a production upload confirms the project's current status.

### AutoRig Runtime Storage

AutoRig intentionally keeps generated task assets on disk so the public site
stays populated:

- `/srv/autorig/data/static/tasks`: cached public task downloads.
- `/srv/autorig/data/static/glb_cache`: cached model files for fast viewing.

Generated assets live under `/srv/autorig/data`, outside the release tree, and
nginx aliases them in; that is why a deploy can replace `current` without
losing them.
- `/var/autorig/videos`: cached task preview videos.
- `/var/autorig/uploads`: original uploaded source files.
- `/var/autorig/preflight-renders`: preflight poster/render files.

Do not delete these by age. Cleanup must be pressure-based: only run when root
free space is below the configured critical threshold. Prefer removing
regenerable ZIP bundles and old terminal-task upload originals before deleting
public task cache, GLB cache, videos, posters, or database task rows.

### AutoRig Deploy

Static-only changes:

```bash
ssh autorig-vps
CUR=$(readlink -f /srv/autorig/current)
sudo cp -a "$CUR" "$CUR-<change>"
sudo cp /tmp/<file> "$CUR-<change>/autorig-online/static/<file>"
sudo chown autorig:autorig "$CUR-<change>/autorig-online/static/<file>"
sudo ln -sfn "$CUR-<change>" /srv/autorig/current.new
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
curl -fsS https://autorig.online/gallery >/dev/null
```

A release is staged beside the live one and `current` is repointed at it, so
nothing is ever edited in place and a rollback is repointing the symlink back.
The deployed tree can be ahead of a local branch: carry only the changed files
rather than shipping a branch wholesale. Mirror whatever was deployed back into
`R:\autorig`, commit it, and push it.

Backend Python or dependency changes:

```bash
ssh autorig-vps
# stage a release as above, copy the backend files in, then:
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
sudo systemctl restart autorig-storage.service
systemctl status --no-pager autorig-storage.service
```

nginx config changes:

```bash
nginx -t
systemctl reload nginx
```

Useful health checks:

```bash
systemctl is-active autorig-storage.service nginx.service
curl -fsS 'http://127.0.0.1:8200/api/gallery?per_page=1&sort=date' >/dev/null
curl -fsS https://autorig.online/gallery >/dev/null
```

## worker-4090 Is This Computer

The render worker `worker-4090` (RTX 4090) is the owner's local workstation,
the same machine that holds `R:\autorig`. It is not a farm box and has no farm
SSH port: operate it locally.

* Runtime: ComfyUI 0.37.0 in `R:\autorig\.runtime\onlyrender`, listening on
  `127.0.0.1:8988`, exposed to the VPS by a reverse tunnel
  `ssh -R 19409:127.0.0.1:8988 autorig-vps` (renderfin sees it as
  `http://127.0.0.1:19409`).
* Controller: `autorig-online/deploy/onlyrender/worker-4090.ps1 -Mode
  Start|Stop|Status` (delegates to `worker-4090-v037.ps1` while
  `WAN2_PROMOTED` exists; `-Legacy` operates the old 0.21.1 runtime).
* Owner's desktop shortcuts: `worker-4090 START.bat` and
  `worker-4090 STOP.bat` on `C:\Users\user\Desktop`, calling the same
  controller.
* The GPU belongs to the owner first. After a reboot the worker is offline
  until started; `Stop` marks it offline, drains accepted renders, then frees
  the GPU. Do not start it without the owner's go-ahead.

## Other Custom VPS Services

The same VPS also hosts these custom services:

- `qwerty-gateway.service`: QwertyStock VPN Gateway, active, code at
  `/root/qwerty_vpn/gateway`, listens on `127.0.0.1:5000`.
- `qwerty-3proxy.service`: QwertyStock VPN 3proxy, active, config at
  `/root/qwerty_vpn/proxy/3proxy.cfg`, public proxy ports are in the
  `49152-49209` range.
- `renderfarmerbot.service`: RenderFarmer Telegram Bot, active, command
  `/usr/bin/python3 /root/renderfarmerbot.py`.
- `renderfarmer-watchdogg.service`: RenderFarmer watchdog, installed; it was
  observed in `activating auto-restart`, so inspect before relying on it.
- `cgtrader_submit.service`: CGTrader Submit Server, installed but observed
  inactive/dead, code at `/root/CGTrader_SUBMIT_SERVER`.
- `qwerty-autoreboot.service`: installed but observed inactive/dead.

Infrastructure services include nginx, ssh, cron, zabbix-agent, systemd
network/resolved/timesync, snap/cups, and QEMU guest agent.

Do not touch unrelated services when the request is about AutoRig only.

## Safety Rules

- Do not use production paths by guesswork; verify with `systemctl cat`,
  `readlink -f /etc/nginx/sites-enabled/...`, `ss -ltnp`, and `git status`.
- Do not run destructive git commands such as `git reset --hard` or
  `git checkout --` unless the user explicitly asks.
- If the repo is already dirty, preserve unrelated changes and stage only the
  files required for the task.
- Do not bypass login, captcha, 2FA, account lock, rate limits, or
  anti-automation screens during live tests.
- For frontend changes, verify live production HTML/JS with `curl` and browser
  testing against `https://autorig.online` when the UI behavior matters. Do not
  create local preview servers for AutoRig UI QA.
- Task viewer theme/backdrop assets are 16:9, not 9:16. Keep source JPGs in
  `static/env/backdrops/source/`, generate viewer derivatives as 16:9 JPGs
  (`1280x720` as the current target), and generate tiny strip thumbnails as
  16:9 JPGs (`160x90`). Do not replace them with vertical derivatives.

### SEO-Critical Layout

Google, Bing, Yandex, and other search engines are the primary traffic source.
Public navigation, footer links, and other crawl-critical internal links must be
present in the initial HTML returned by the server. Do not make SEO-critical
header/footer links depend only on client-side JavaScript rendering. Shared
layout partials are the canonical source for public header/footer markup; JS may
only enhance that markup for auth state, credits, language, theme, and menu
behavior.
