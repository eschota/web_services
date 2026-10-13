# V3 task completion incident, 2026-10-13

Owner's screenshot: five recent tasks started, no terminal Telegram messages.

## Confirmed causes

The queue was empty, with no AutoRig task in processing. All five screenshot
tasks had 3/3 output records and terminal V3 state `needs_review`, not `done`.
Numeric deformation checks failed. Startup statistics omitted this state:
1470 done + 339 error + 239 needs_review = 2048 total at the audit snapshot.

The deployed `telegram_bot.py` also had a corrupted function boundary: the
completion body was spliced into `broadcast_task_error`; `broadcast_task_done`
and its preview helpers were absent. V3 notifications failed with NameError
after reserving `telegram_done_notified_at`. Failures preceded the 15:57 restart,
so that restart was not their cause.

## Repair and verification

- Terminal delivery restored in an isolated `telegram_task_completion.py`;
  shared public functions forward to it. Resolve the completion module before
  recording the send reservation. Keep the established video-only policy.
- `needs_review` appears in startup totals and a review outcome uses the yellow
  `Task needs review` heading. It never changes the task to `done` or approves a
  bad rig. Failed video sends do not become text completion notices.
- Production tests: seven terminal-delivery tests; backend/base gate PASS,
  eight checks including the new persistent `backend_terminal_notifications`
  corpus case. Report: `20261013T165818Z-backend-base.json`.
- Live smoke on the real drZomboss task found its real V3 video and retained the
  review warning. All network sends and notification writes were stubbed; this
  was functional smoke, not proof of Telegram receipt or owner acceptance.
- Active release at deployment: `live-20261013e-postfooter-qa-label`;
  autorig-storage PID 4018299; autorig-mt PID 2628551 remained unchanged.
  Verified actual process cwd and public fleet/task-page endpoints.
- Renderfin dry-run confirmed `restart_policy_string=resume`; queued/running
  graphs remained present after promotion. The legacy wipe flag is still `1`,
  but live main.py consults it only in the explicit restart-policy `wipe`
  branch. No environment, account, permissions policy or secret was changed.

## Remaining rig work

Read-only detailed evidence stays in project `.work/task-completion-20261013/`.
Five runs: `3190dd0a5a9c7299f3d3`, `4ed5f19937dee5b520ab`,
`53d26f043042e7476cb7`, `88edbcd0c3fe79ed2f3d`, `a4f7fc4407111a09a462`.

Common-cause audit: all five original inputs were unskinned/unanimated and used
the same geometric fitter (`v3.2026-10-13.8`, rig-path fingerprint `22bed47c329d`).
Bind identity errors stayed within their tolerances. The shared failure is
incorrect anatomical ownership on continuous/contacting surfaces: body vertices
receive limb weights. Tendenz is the clean control, with no divergent coincident
aliases but 89–180x stretch; wrong pairs include front leg/spine and hips/hind leg.
Different weights at coincident vertices amplify four other cases, so averaging
aliases alone is not a universal fix. Continue the existing Fable skin context
in `limb_reach`, `geodesic_assign`, `contact_split` and `weld_split`.
Detailed evidence: project `common-skin-cause.md`; no customer model was changed.

- Angel: catastrophic skin stretch/detachment and strong left-arm collision.
- Both cats: tears/detachment; one also has Tail3 outside the mesh.
- Cyborg: gross skin stretch plus Hips/Spine outside samples.
- drZomboss: almost clean small rig-check, but full clips detach by 11.38% H
  and expose severe slit/tear metrics. The full-pose check must remain.
- Layer detection currently uses words in Vision material descriptions. `fur`
  and `fabric-like texture` do not prove a separate physical layer. Fix that
  detector using vertex/geometry evidence, while keeping numeric failures fatal.
- Referenced PBR files require an actual embedded-texture audit before calling
  this source texture loss; filename presence alone is insufficient.

Current problem video for drZomboss sent to DEV as diagnostic-only media
`74052a74754d`, Telegram 7155. No corrected rig or per-task owner acceptance is
claimed. Old missed notifications were not manually replayed into other chats.
