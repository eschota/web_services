# Gotchas

Non-obvious behaviour that cost real time to discover. Each entry says what is
surprising, how it shows up, and what to do about it.

## Contents

- [Blender exits 0 when its script raises](#blender-exits-0-when-its-script-raises)
- [The vertex-PBR bake is 30x slower on Blender 5.1](#the-vertex-pbr-bake-is-30x-slower-on-blender-51)
- [The converter serves from a process that outlives your deploy](#the-converter-serves-from-a-process-that-outlives-your-deploy)
- [A Blender script must not import the server's package](#a-blender-script-must-not-import-the-servers-package)
- [The negative prompt reaches no model](#the-negative-prompt-reaches-no-model)
- [The words "glass" and "glasses" are deleted](#the-words-glass-and-glasses-are-deleted)
- [server-status does not check the bearer token](#server-status-does-not-check-the-bearer-token)
- [A worker's status_url drops its port](#a-workers-status_url-drops-its-port)
- [sendMediaGroup returns a list](#sendmediagroup-returns-a-list)
- [A bot cannot delete a message older than 48 hours](#a-bot-cannot-delete-a-message-older-than-48-hours)
- [callback_data is capped at 64 bytes](#callback_data-is-capped-at-64-bytes)
- [A stage deadline must not restart with the service](#a-stage-deadline-must-not-restart-with-the-service)
- [The GLB cache holds the last copy of deliverables](#the-glb-cache-holds-the-last-copy-of-deliverables)
- [Test-suite traps](#test-suite-traps)

---

## Blender exits 0 when its script raises

`blender --background --python script.py` returns **exit code 0** even when the
Python script raises. The traceback goes to stdout and the process reports
success.

This is why the converter's Vertex-PBR post-processor could fail silently: the
adapter checks `completed.returncode != 0`, that check never fires, and the only
symptom that reaches renderfin is "manifest is missing" — the actual error is
swallowed.

Verified directly on f13: a deliberately bad input produced
`VertexPBRError: Input must be an existing GLB`, no manifest, and `EXIT=0`.

**When calling Blender in background mode, never trust the exit code.** Check
for the artifact the script was supposed to produce, and carry the stdout tail
into the error when it is missing.

## The vertex-PBR bake is 30x slower on Blender 5.1

Measured on f13, same input, same box: **62 seconds on Blender 4.3 against more
than 35 minutes on 5.1**, both producing a valid v5 manifest.

Every farm box has 4.3 and 5.1 installed, and `_resolve_server_blender_path`
returns the newest it finds, so the bake had been running on 5.1. The script's
own docstring says "Run with Blender 4.3 or newer" and every known-good
manifest on the boxes records `blender_version: 4.3.2` — the 5.1 path was never
the validated one, it was simply first in the candidate list.

`autorig_hunyuan/adapter.py` now picks 4.3 for this step specifically
(`_vertex_pbr_blender`), with `HUNYUAN_BLENDER_PATH` to override. The rest of
the converter still uses whatever the server resolved, which is correct — only
the Hunyuan post-process was written against 4.3.

When a queue of 3D jobs is draining far slower than the ~10 minutes a
generation actually takes, check which Blender the bake is running under:

```powershell
Get-CimInstance Win32_Process -Filter "Name='blender.exe'" |
  ForEach-Object { $_.CreationDate.ToString("HH:mm:ss") + "  " + $_.ExecutablePath }
```

## The converter serves from a process that outlives your deploy

Copying a file to a box and restarting its scheduled task does **not**
necessarily replace the process serving the converter port. Every box was found
serving from a process started days earlier, holding the old module in memory,
while the scheduled-task restart had started additional processes alongside it.

Check the process that actually owns the port, not any process matching the
name:

```powershell
$conn = Get-NetTCPConnection -State Listen -LocalPort <converter port>
(Get-Process -Id $conn.OwningProcess).StartTime
```

Compare that to the mtime of the file you deployed. If the process is older,
your change is not running, no matter what the file on disk says.

`/api-converter-glb-restart-server` is the intended restart (auth via
`X-GLB-Admin-Token`, the token is at `%LOCALAPPDATA%\AutoRig\converter_admin_token`),
but it returns 409 while any converter task is active — and on a busy farm that
window is very hard to catch. Plan a restart for a quiet period rather than
expecting to slip one in.

## A Blender script must not import the server's package

`autorig_hunyuan/vertex_pbr.py` runs inside Blender's bundled Python, which has
no `psutil`. Importing `autorig_hunyuan.fbx_contract` through the package
executes `autorig_hunyuan/__init__.py`, which imports `.adapter`, which imports
`psutil` — and the script died at module scope for a full day before anyone
could see why, because of the exit-code trap above.

Two rules follow. A Blender-side module loads its dependencies **by file path**,
never through the package. And when loading by path, register the module in
`sys.modules` before `exec_module`: `@dataclass` resolves its own module through
`sys.modules[cls.__module__]` and dies on the first frozen dataclass otherwise.

## The negative prompt reaches no model

`renderfin/assets/workflows/t_pose.json` contains no `$negative_prompt`
placeholder at all — the only placeholders are `$image`, `$prompt`,
`$output_url` and `$output_url_`. Separately, node 29 `ConditioningZeroOut`
takes its input from the *positive* encode and zeroes it, and both the
ControlNet apply and the refine sampler use that as their negative. Flux-schnell
also runs guidance-free.

So anything written into `negative_prompt` for a t_pose render is inert. Worse
than useless: asking an LLM to write one teaches it to think in negations, which
then leak into the positive prompt as literal nouns — and Flux draws every noun
it is given.

`DEFAULT_NEGATIVE_PROMPT` still exists because other workflows do read it. For
t_pose, express every exclusion as a positive statement.

## The words "glass" and "glasses" are deleted

`renderfin/templating.py` strips `\bglass(?:es)?\b` from every prompt before
substitution, for parity with the original C# server. A character described with
spectacles silently loses the word and renders an undefined face region.

Write "spectacles", "goggles", "visor" or "lens" instead. The shipped LLM
instruction says so explicitly.

## server-status does not check the bearer token

`/api-converter-glb/server-status` answers 200 regardless of the credential it
is given. Only `/api-converter-glb/generate-3d` validates the bearer.

This is why both boxes once looked healthy while nothing could be submitted. To
prove credentials work, POST a deliberately invalid body to `generate-3d` and
treat the resulting `400 invalid_request` as success — a 401/403 means the token
is stale.

## A worker's status_url drops its port

The converter builds `status_url` from the Host header and loses the port, so
what it returns can point at an unrelated service. `hunyuan_client.submit()`
re-bases the path onto the worker's own origin. Do not use the advertised URL
as-is.

## sendMediaGroup returns a list

Every other Bot API send method answers with one Message object;
`sendMediaGroup` answers with a **list** of them. Reading `.get("message_id")`
off the result raises `AttributeError`.

`_message_ids()` in `renderfin/telegram_delivery.py` normalises both shapes.
Use it rather than indexing the result directly — the two-variant album depends
on capturing both photo ids so they can be cleaned up later.

## A bot cannot delete a message older than 48 hours

Telegram allows a bot to delete its own message only inside a 48-hour window.
An id without a send time cannot be filtered before the call, so every sweep
would retry ids the API will always refuse.

Recorded messages therefore carry a timestamp (`SentMessage.at`), are dropped
locally past ~47h, and a refusal is remembered in `telegram_undeletable` so it
is never asked again.

## callback_data is capped at 64 bytes

Inline button payloads must fit in 64 bytes. The scheme in use is
`<prefix>:<job-uuid>[:<variant>]` — e.g. `rfa:11111111-2222-3333-4444-555566667777:b`
is 42 bytes. Prefixes: `rfg` generate, `rfa` approve variant, `rfr` regenerate,
`rfe` resume, `rfs` submit, `rfd` discard.

The approve pattern lives in one constant, `_APPROVE_PATTERN` in
`telegram_bot.py`, used by both the handler registration and the parser so they
cannot drift.

## A stage deadline must not restart with the service

The Hunyuan ceiling is 4 hours, but a deadline computed inside the wait loop is
recomputed every time a service restart re-enters the stage. One production job
accumulated **20.7 hours** at one stage out of five fresh four-hour windows and
never reached its retry path.

The stage start is stamped on the job (`stage_started_at` + `timed_stage`) and
persisted, so a restart resumes the same window. A genuine retry, resume or
regenerate clears it — those have earned a fresh one.

## The GLB cache holds the last copy of deliverables

Workers purge their outputs, so a cache entry is frequently the **only**
surviving copy of a user's deliverable. An eviction pass that deletes by age
destroys data. This happened: 236 cache files were deleted and roughly 80% had
no upstream copy, affecting 251 tasks.

An entry is now deleted only after its upstream URL answers 200.

The second-order trap: the candidate list is oldest-first and the oldest entries
are exactly the ones whose upstream is long gone, so a fixed probe budget was
spent on the same files every run and the pass freed nothing while the cache sat
40% over its cap. Verdicts are remembered in
`/var/autorig/glb_cache_last_copy.json`, expiring after a week so a rebuilt
worker can start serving again and the memo never hardens into a blacklist.

## Test-suite traps

**Do not share one sqlite file between the queue and two managers.** A test that
opens a second manager while the queue still holds the first blocks for the full
`busy_timeout` and fails on "database is locked". Give the queue its own path.

**Tests must not read the live farm config.** On a machine that has
`/etc/autorig-renderfin-hunyuan.json`, the Hunyuan stage takes the converter-API
path and reaches for the network. The `_Env` fixture patches
`HUNYUAN_WORKERS_FILE`, `HUNYUAN_WORKERS` and `HUNYUAN_API_TOKEN`.

**Do not mutate `job.stage` on a job whose runner is live.** `_run` is still
choosing a branch; changing the stage under it makes it fall through into the
real stage and hit the network. Use the `_idle_job` helper, which registers a
job without spawning a runner.

**The last-copy memo is a real file on a real path.** Without redirecting it per
test, one test's verdicts become another's and a probe-counting test sees none.
`GlbCachePruneTests.setUp` patches `LAST_COPY_MEMO_PATH`.

**`Task.ready_urls` is a Python property, not a column.** `select(Task.ready_urls)`
raises `ArgumentError`. Select `Task._ready_urls` and `json.loads` it.
