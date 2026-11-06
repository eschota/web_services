# AutoRig AI node (`ai_node.py`)

A standalone service that serves the backend's Vision and Text LLM jobs on a
render box **without the GLB converter**. It is meant for boxes that run
ComfyUI (worker-4090, f12, f5, f7 …), so it gives the GPU back whenever
ComfyUI wants it.

* `ai_node.py` is the service. It needs Python 3.10 or newer and uses only the
  standard library, so there is nothing to `pip install`.
* `selftest.py` starts the service against a fake ComfyUI, a fake llama-server
  and a fake nvidia-smi, then checks the contract and the busy/unload policy.
* `config.example.json` is the worker-4090 setup, with paths checked on
  2026-10-07.

## What the backend sees

The backend (`backend/ai_vision_api.py`) treats this node exactly like a
converter node. Every route lives under `/api-converter-glb` and needs
`Authorization: Bearer <token>`:

| Route | Answer |
|---|---|
| `GET /server-status` | `tasks_summary{queue_size,pending,processing}`, `ai_models{loaded_model, models[{id,context_tokens,vision,system_prompt_supported,unlimited_output_supported}], system_prompt_supported, system_prompt_models, unlimited_output_supported}`, `processing_tasks[{workload_class:"ai_vision", mode}]`. Diagnostics are under `ai_node`. |
| `POST /ai-vision` `{prompt, image_url, max_output_tokens, model, system_prompt?}` | `202 {task_id, status:"Pending", status_url, mode, model}`. `400 invalid_request` means a bad payload. `503` means busy or the wrong model. |
| `POST /text2text` `{prompt, max_output_tokens, model, system_prompt?}` | Same as `/ai-vision`. |
| `GET /ai-vision/status/{id}` | `{status: Pending\|Processing\|Completed\|Failed, answer, reasoning, error, elapsed_seconds, current_stage, mode, model, model_usage}`. An unknown id returns 404. |
| `GET /healthz` | `{"status":"ok"}`. No token is needed; this is for local checks only. |

### Behaviour copied from the converter

The following comes from converter main `60101f4` (`bonsai_adapter.py` and
`webserver_converter_glb.py`). Each copied function says where it came from.

**Validation**
* The prompt is required, and the prompt plus the system prompt may be at most
  8000 characters.
* `max_output_tokens` is clamped to the model's ceiling. `-1` means unlimited
  and is passed to llama-server as `max_tokens: -1`. If it is missing, the
  ceiling is used.
* A model id other than the pinned one gets
  `503 {"error":"runtime_not_installed","available_models":[…]}`.

**llama-server launch line**
* `-m <weights> [--mmproj] [--reasoning on|off] --alias <id> -ngl <n> -c <ctx> --host 127.0.0.1 --port <p> --jinja`.
* Startup is polled on `/health` for up to 300 s.
* Shutdown is terminate, then wait 20 s, then kill.

**Chat request**
* A system message is sent if there is a system prompt.
* The user content is the plain prompt, or for vision `[text, image_url data:<type>;base64]`.
* `temperature` is 0.3 and the request timeout is 900 s.
* The answer is `content`; the reasoning is `reasoning_content`.
* An empty answer fails the task with
  `model_spent_its_budget_thinking: … raise max_output_tokens and retry`.

**Images**
* The same SSRF guard is used: http(s) only, ports 80/443, public IPs only, and
  no credentials in the URL.
* The download is capped at 16 MiB, must be `image/*`, and uses the
  `AutoRig-AIVision/1` user agent.

**Catalogue**
* `system_prompt_models` lists the model only if it is `bonsai2-27b`. That is
  the only model that passed the system-role canary, and the converter
  hard-codes the same list.
* Qwen therefore gets its instructions folded into the prompt by the backend,
  as it does today.
* The `system_prompt_supported` config key overrides this.

### Differences from the converter, on purpose

* **One process, one pinned model, no swapping.** The catalogue lists only
  that model. The backend already treats a node's catalogue as a hard filter,
  so no backend change is needed to keep Qwen jobs away from a Bonsai node.
* **The ComfyUI-aware VRAM policy** described below.
* **`data:image/...;base64,` URLs** are accepted as `image_url` and decoded
  locally, under the same size and type rules. The backend does not send these
  today, because it publishes inline images as URLs first.
* **Every redirect target goes through the SSRF guard again.** The converter's
  plain `urlopen` does not do this.
* **Video:** the backend turns a `video_url` into a contact-sheet or
  first-frame image before it calls a node. The node never sees video, and the
  converter doesn't either.
* **Not implemented:** the idempotency ledger, GPU-arbiter leases and
  preemption. Tasks live in memory only, so a restart forgets them and their
  status returns 404.

## GPU policy: ComfyUI comes first

The node polls ComfyUI every `comfy_poll_seconds`, which defaults to 3 s.
* It reads `GET /prompt` → `exec_info.queue_remaining`, which counts running
  plus pending prompts.
* If `/prompt` doesn't answer, it falls back to `GET /queue`, which carries
  every queued workflow and so is larger.

| Situation | What the node does |
|---|---|
| ComfyUI has a prompt running or queued | Reports `queue_size: 99` and `accepting_ai_vision: false`, so the backend prefers other nodes. New submits get `503 gpu_busy_comfyui`. An **idle** llama-server is stopped at once. |
| A generation is running when ComfyUI starts | It finishes; it is not killed. The model is unloaded right after it. |
| A task is queued while ComfyUI is busy | It waits with `current_stage: waiting_for_gpu` and `wait_reason`. After `comfy_wait_seconds` (300) it fails with `gpu_unavailable: …`. |
| ComfyUI starts while the weights are loading | The half-loaded server is stopped. The task waits, then loads again. |
| The model is not loaded and free VRAM is below `need + margin` | Reports 99. Submits get `503 insufficient_vram`. |
| nvidia-smi can't be read | Submits get `503 vram_unknown`, but only when a cold load would be needed. |
| Model loaded and idle for `keepalive_seconds` (120) | llama-server is stopped. |
| `max_tasks` (2) tasks already accepted | `503 queue_full`. Tasks run one at a time. |
| ComfyUI unreachable | Treated as idle, because a ComfyUI that isn't running holds no VRAM. The VRAM check still applies. Set `comfy_unreachable_is_busy: true` to treat it as busy instead. |

**VRAM figures**
* "need" is `vram_need_mb`. If you leave it out, it is estimated as the
  weights and mmproj file sizes plus `vram_overhead_mb` (1536).
* The margin is `vram_margin_mb` (1536).
* Measured on f13: Qwen 9B at `-c 8192` used about 8.7 GB, and Bonsai at
  `-c 4096` about 9.3 GB. That is why `vram_need_mb` is 9400 for Bonsai, and
  about 8800 would suit Qwen.

**Things to know about this policy**
* The node never asks ComfyUI to free its model cache. If ComfyUI keeps 15 GB
  or more cached on a 24 GB card, the LLM stays refused until that memory is
  released. This is by design.
* A 503 from this node shows up in the backend as `worker_busy`, and the
  backend does not retry it on another node. The high `queue_size` is what
  steers jobs away beforehand.

**Orphans**
* On Windows, llama-server runs in a kill-on-close job object. Killing
  `ai_node` (for example with Task Scheduler "End", or a crash) also kills
  llama-server.
* If the llama port is still taken at load time, or at startup, the node stops
  only a process whose command line carries **our** weights, `--port` **and**
  `--alias`.
* Anything else on that port is left alone, and the task fails with
  `llama_port_in_use`.

## Install on a Windows box

The commands below are for PowerShell. Paths use `C:\ProgramData\AutoRig\ai-node`.

**The GPU belongs to its owner.** On worker-4090, do not register or start the
node without the owner's go-ahead.

1. **Copy the files** from this folder to `C:\ProgramData\AutoRig\ai-node\`:
   `ai_node.py`, `selftest.py`, and `config.example.json` saved as
   `config.json`. Copy them from our own boxes or the repo; nothing is
   downloaded.

2. **Python.**
   * Any 3.10 or newer works, since only the standard library is used.
   * On worker-4090: `py -3.11`, or the default `python` 3.10.
   * On a ComfyUI-portable box, its `python_embeded\python.exe` works as well.
   * The scheduled task wants `pythonw.exe`, so that no console window opens.
     Find it with:
     ```powershell
     py -3.11 -c "import sys,os;print(os.path.join(os.path.dirname(sys.executable),'pythonw.exe'))"
     ```

3. **Choose the llama.cpp build and weights.** Copy over the LAN from our own
   boxes only.

   | Box | GPU | llama-server | Weights |
   |---|---|---|---|
   | worker-4090 | RTX 4090 24 GB (Ada) | `C:\llm\bonsai\bin\llama-server.exe`, the CUDA 13 build already there (checked) | `C:\llm\bonsai\models\bonsai2-27b-PQ2_0.gguf` + `mmproj-Q8_0.gguf` (checked). Same sha256 as f13's `bonsai2-PQ2_0.gguf`; only the file name differs. No Qwen files exist at home. |
   | f7, f13 (Pascal) | GTX 1080 Ti 11 GB | f13's CUDA 12 build `C:\ProgramData\AutoRig\ai-vision\bin` (b10709, 1.17 GB). CUDA 13 has dropped Pascal, so the 4090 build will not run here. | f13's `C:\ProgramData\AutoRig\ai-vision\models\` |
   | f5 | RTX 3070 Ti 8 GB | — | Neither model fits at `-ngl 99` with the margin, so the node would always report `insufficient_vram`. Not a candidate. |
   | f12, f15, raptor | not probed | — | Measure free VRAM with ComfyUI idle first. |

   * Use `llama_port: 8092`. Port 8091 is used by the converter's AI worker,
     and on worker-4090 by the owner's manual `C:\llm\bonsai\start.sh` and
     `use.sh`.
   * Run only one model per box. Two nodes, each with its own model, would
     compete for the same VRAM.

4. **Token.** Generate one into the token file and lock the file down. It is
   never logged. Changing the file takes effect without a restart.
   ```powershell
   $dir = 'C:\ProgramData\AutoRig\ai-node'
   py -3.11 -c "import secrets,pathlib;pathlib.Path(r'$dir\token.txt').write_text(secrets.token_urlsafe(32))"
   icacls "$dir\token.txt" /inheritance:r /grant:r "${env:USERNAME}:(R)" "*S-1-5-18:(F)" "*S-1-5-32-544:(F)"
   ```
   * The same value goes into the VPS workers-file entry's `"token"` (see
     below). Copy it by hand; never paste it into chat or logs.
   * If no file is configured, the environment variable `AI_NODE_TOKEN` is
     used instead.

5. **Check the setup without loading anything.** This reads the files, runs
   nvidia-smi and calls ComfyUI's `/prompt`. It prints the launch line and
   whether the node would accept work. It exits with 0 only when the runtime is
   installed and a token is set.
   ```powershell
   py -3.11 "$dir\ai_node.py" --config "$dir\config.json" --check
   ```

6. **Self-test.** This uses fakes on free ports and never touches the GPU or
   real models. It takes about 100 s.
   ```powershell
   py -3.11 "$dir\selftest.py"
   ```

7. **Run it in the foreground once, then probe it.** This step loads the model
   only when a job arrives.
   ```powershell
   py -3.11 "$dir\ai_node.py" --config "$dir\config.json"
   # in another window:
   $t = Get-Content "$dir\token.txt"
   curl.exe -s -H "Authorization: Bearer $t" http://127.0.0.1:5480/api-converter-glb/server-status
   curl.exe -s -H "Authorization: Bearer $t" -H "Content-Type: application/json" `
     -d '{\"prompt\":\"Name three colours.\",\"max_output_tokens\":64}' `
     http://127.0.0.1:5480/api-converter-glb/text2text
   curl.exe -s -H "Authorization: Bearer $t" http://127.0.0.1:5480/api-converter-glb/ai-vision/status/<task_id>
   ```

8. **Scheduled task at logon.** It runs in the same user session as ComfyUI,
   restarts every minute if it dies, and has no time limit.
   ```powershell
   $py = py -3.11 -c "import sys,os;print(os.path.join(os.path.dirname(sys.executable),'pythonw.exe'))"
   $action   = New-ScheduledTaskAction -Execute $py -Argument "`"$dir\ai_node.py`" --config `"$dir\config.json`"" -WorkingDirectory $dir
   $trigger  = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
   $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 `
               -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew `
               -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
   Register-ScheduledTask -TaskName 'AutoRig AI Node' -Action $action -Trigger $trigger `
     -Settings $settings -RunLevel Limited -Description 'AutoRig AI node: LLM jobs, yields the GPU to ComfyUI'
   Start-ScheduledTask -TaskName 'AutoRig AI Node'
   ```
   * To stop it: `Stop-ScheduledTask -TaskName 'AutoRig AI Node'`. The job
     object takes llama-server down with it.
   * To remove it: `Unregister-ScheduledTask -TaskName 'AutoRig AI Node' -Confirm:$false`.
   * On worker-4090, the owner may prefer to hook Start/Stop into
     `deploy/onlyrender/worker-4090.ps1` instead, next to the ComfyUI worker.

9. **Logs.** `ai-node.log` rotates at 5 MB and keeps 3 files. If
   `llama_log_file` is set, it holds the last llama-server run only. Prompts
   and tokens are never logged; only their lengths are.

## Wiring a node into the backend (VPS, by an operator)

The node listens on `127.0.0.1` only. The VPS reaches it through an SSH tunnel.

1. **Tunnel.**
   * **Farm box:** add a line to `/srv/autorig/secrets/farm-tunnels.conf`:
     `<name> <ssh_port> <vps_port> 5480`. Restarting
     `autorig-storage-tunnels.service` drops all farm tunnels for a moment. A
     separate unit, like the raptor tunnel, avoids that.
   * **Home box:** use a reverse tunnel, for example
     `ssh -R 15410:127.0.0.1:5480 autorig-vps` on worker-4090. Port 15410 is
     free.
   * **Home box helper:** also add the port to the allowlist in
     `/home/debian/fleet4090/clear_stale_port.sh`.

2. **Workers-file entry** in `/srv/autorig/secrets/renderfin-hunyuan.json`.
   It is hot-reloaded and needs no restart:
   ```json
   {"name": "worker-4090-ai", "physical_node": "worker-4090", "url": "http://127.0.0.1:15410",
    "token": "<contents of token.txt>", "enabled": false, "canary_approved": false,
    "capability_mode": "ai_only", "disabled_reason": "AI node only (ai_node.py), no Hunyuan",
    "notes": "bonsai2-27b pinned; yields the GPU to ComfyUI"}
   ```
   * `enabled: false` and `canary_approved: false` keep renderfin and the
     site's 3D picker from sending it Hunyuan work.
   * Any `capability_mode` other than `full`/`comfy_ai` keeps renderfin's
     converter and lease logic away.
   * The backend's AI routing ignores all three fields, unless the entry says
     `ai_vision_enabled: false`.
   * `physical_node` must be unique in that file, because the backend keeps the
     first entry per node. f12 and raptor already have entries, so their AI
     node needs a different `physical_node` (for example `f12-ai`), or the
     backend's `ai_url` change described in the routing report.

3. **Check from the VPS:**
   ```bash
   curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:15410/api-converter-glb/server-status | jq .ai_models
   ```

## Config reference

Relative paths resolve against the folder that holds `config.json`. When
`model_id` is `bonsai2-27b` or `qwen35-9b-uncensored`, any model key you leave
out takes the converter's value: title, file names, context, ceiling, reasoning
and uncensored.

| Key | Default | Meaning |
|---|---|---|
| `node_name` | hostname | Reported as `physical_node`. |
| `listen_host` / `listen_port` | `127.0.0.1` / `5480` | The node's API. |
| `token_file` | `token.txt` | Bearer token, re-read when it changes. Falls back to the `AI_NODE_TOKEN` environment variable. |
| `model_id` | required | The single pinned model id, as the backend names it. |
| `weights` / `mmproj` | known file names | GGUF paths. Relative paths resolve under `models_dir`, which defaults to `<config dir>\models`. |
| `context_tokens` / `max_output_tokens` | per model | `-c` and the output ceiling. |
| `reasoning` | per model (`auto`) | `on`/`off` adds `--reasoning`; `auto` leaves the template alone. |
| `system_prompt_supported` | `true` only for `bonsai2-27b` | Whether the model is listed in `system_prompt_models`. |
| `vram_need_mb` | files + `vram_overhead_mb` | VRAM the loaded model takes. |
| `vram_overhead_mb` / `vram_margin_mb` | 1536 / 1536 | Used for the estimate, and the headroom required to load. |
| `llama_server` | required | Path to `llama-server.exe`, or an argv prefix list (the self-test uses one). |
| `llama_port` | 8092 | llama-server port, on loopback only. |
| `ngl` | 99 | `-ngl`. |
| `extra_llama_args` | `[]` | Appended to the launch line, for example `["--flash-attn","on"]`. Qwen3.5 nodes: `["--presence-penalty","1.5","--top-k","20","--top-p","0.8"]` (see "Sampling for Qwen3.5"). |
| `llama_log_file` | empty (discard) | llama-server output for the last run. |
| `cuda_visible_devices` / `gpu_index` | empty / 0 | GPU selection for llama-server and for nvidia-smi `-i`. |
| `nvidia_smi` | `nvidia-smi` | Command, or an argv list. |
| `comfy_url` | `http://127.0.0.1:8188` | ComfyUI base URL. Empty means ComfyUI is not watched. worker-4090 uses `:8988`. |
| `comfy_poll_seconds` | 3 | Poll interval for ComfyUI and nvidia-smi. |
| `comfy_unreachable_is_busy` | false | See the policy table. |
| `free_comfy_when_idle` | false | On a render box whose ComfyUI keeps its models in VRAM after a job: when ComfyUI is online and idle and the VRAM is short, accept the job and, right before loading, `POST /free {"unload_models": true, "free_memory": true}` to ComfyUI (at most once per 30 s). ComfyUI 0.37 keeps the weights staged in RAM, so its next render reloads them in seconds. Used on worker-4090 and f12. |
| `converter_status_url` / `converter_token_file` | empty | A converter on the same card (Hunyuan / conversions, e.g. f7's 127.0.0.1:7000, the 4090's 3D adapter 18777): any processing or queued task there counts as busy, like a ComfyUI prompt; an unreadable converter is treated as busy. |
| `comfy_wait_seconds` | 300 | How long a queued task waits for the GPU. |
| `keepalive_seconds` | 120 | How long an idle model stays loaded. 0 unloads it after every task. |
| `max_tasks` | 2 | Accepted-but-unfinished tasks; one runs at a time. |
| `start_timeout_seconds` / `request_timeout_seconds` | 300 / 900 | Same values as the converter. |
| `task_retention_seconds` | 21600 | How long finished tasks stay queryable. At most 1000 are kept. |
| `allow_private_image_urls` | false | **Test only.** Turns off the SSRF guard. |
| `max_prompt_chars` | 8000 | Characters of system_prompt + prompt one request may carry (2026-10-11). The backend's own cap stays 8000; a node that a long-context caller talks to directly (the film director) raises it with its `context_tokens`. |
| `gpu_lease_hold_file` | empty | Two nodes on one card (2026-10-11). The node that needs most of the card writes this file while it waits for, loads or holds its model, and removes it once the card is free again. |
| `gpu_lease_yield_files` / `gpu_lease_fresh_seconds` | `[]` / 60 | A node listing another node's lease file treats a fresh lease (mtime within the window) like a busy ComfyUI. It refuses new work with `503 gpu_leased`, unloads when idle, and lets a running answer finish. |
| `gpu_gate_url` / `gpu_gate_owner` / `gpu_gate_grace_seconds` | empty / node name / 30 | worker-4090's GPU gate (`C:\AI\HY3D2\fleet-adapter\gate.py`, control 127.0.0.1:18778). While the node waits for ComfyUI to drain, loads or answers (plus the grace), it holds a gate lease, so renderfin sends the card no new render. |
| `owner_games` / `owner_games_file` | `[]` / empty | Executables (plus a file with one per line). While one runs the node loads nothing and takes no work. Empty = not watched. The owner, 2026-10-11: «он должен брать мозг пока идёт игра», so both worker-4090 nodes run with it empty. |
| `fit_when_short` / `fit_min_vram_mb` / `fit_target_mb` | false / 0 / 1536 | A model that does not fit whole (2026-10-11: cs2 and the desktop leave the 27B some GB short). With ComfyUI put away and at least `fit_min_vram_mb` + margin free, llama-server starts with `-ngl auto --fit on --fit-target <MiB>`: as many layers on the GPU as fit, the rest in RAM, the same context. `server-status` shows `llama_fit`. |
| `owner_priority` / `owner_preempt_comfy` / `owner_warm_seconds` | false / false / 600 | The owner's priority (2026-10-11: «приоритет с отменой других задач именно для задач с режиссёром или другими личностями на этой ноде»). A request with `"priority": "owner"` is admitted while ComfyUI renders and goes first. `owner_preempt_comfy` deletes ComfyUI's queued prompts and interrupts the running one; renderfin sends each back to Pending on another box under the same task id. For `owner_warm_seconds` after an owner request the node keeps the gate and the model. |
| `converter_503_is_busy` | true | false = the 3D adapter's 503 (it refuses new 3D jobs, e.g. because an LLM took the VRAM) is not «the GPU is in use». |
| `log_file` / `log_level` | `ai-node.log` / `INFO` | Service log. |

A request may also carry `temperature` (0-1.5, default 0.3; 2026-10-11).

## Deployed nodes (2026-10-07)

All pinned to `qwen35-9b-uncensored` (the backend runs with `AI_MODEL_SINGLE`); weights and llama.cpp builds were
copied between our own boxes (f11 → worker-4090 over SSH, then over each site's LAN), nothing downloaded.

| Registry entry | Box | Reached by the VPS through | Lifecycle |
|---|---|---|---|
| `f11-ai` | f11, GTX 1080 Ti, CUDA 12 build | f11 tunnel 15533 → nginx :5533 `location /ai/` → 127.0.0.1:5541 | task `AutoRig AI Node F11` (pythonw, at logon, restarts) |
| `f7-ai` | f7, GTX 1080 Ti, CUDA 12 build | f7 tunnel 15131 → nginx :5131 `location /ai/` → 127.0.0.1:5541 | task `AutoRig AI Node F7`; yields to the converter on 127.0.0.1:7000 |
| `worker-4090-ai` | worker-4090, CUDA 13 build | reverse tunnel 15410 → 127.0.0.1:5480 | `ensure-worker-4090.ps1` every minute (task `AutoRig 4090 AI Node`): runs only while the farm ComfyUI (8988) runs - the card is the owner's first |
| `f12-ai` | f12, RTX 3080 Ti, CUDA 13 build | reverse tunnel 15412 (`f12-reverse-tunnel.ps1`, task `AutoRig F12 AI Tunnel`) → 127.0.0.1:5480 | `ensure-f12.ps1` every minute (task `AutoRig F12 AI Node`): runs only while the ComfyUI backend (8289) runs |

The farm tunnel key may only open each box's nginx port, hence the `/ai/` location on f11 and f7 (the converter
route next to it is untouched). `max_tasks` is 8 on f11/f7 and 4 at home: with 2, bursts of parallel requests got
`503 queue_full`, which the backend sees as `worker_busy`.

## Sampling for Qwen3.5 (2026-11-07, live on f2-ai, f11-ai, f13-ai, worker-4090-ai)

Every Qwen3.5 node carries Qwen's non-thinking sampling defaults as llama-server defaults:

```json
"extra_llama_args": ["--presence-penalty", "1.5", "--top-k", "20", "--top-p", "0.8"]
```

The node still sends only `temperature` (0.3) per request, so these become the server's values for everything else.
Before them the 9B often repeated the same keywords until the 640-token cap, so the JSON never closed and M+ had to
make a second call for title/description. M+ prompt, 10 images × 2, f13-ai (CUDA 12 build), backend `/api/vision`:

| f13-ai, 9B | strict JSON (= has title) | 50 keywords saved | duplicate keywords (median) | p50 |
|---|---|---|---|---|
| before, 640 tokens (n=17) | 41 % | 71 % | 14 | 7.3 s |
| after, 640 tokens (n=19) | 58 % | 100 % | 2 | 7.2 s |
| after, 768 tokens (n=11) | 82 % | 73 %* | 0 | 4.3 s |

\* sampling noise: an answer that completes under 640 completes the same way under 768; a cap only cuts.

Applied with `set_sampling.ps1`-style edits: back up `config.json` (`config.json.bak-sampling-<time>`), set the
node-wide `extra_llama_args`, restart the node when it is idle (`ensure-*.ps1` boxes: stop the ai_node process, the
minute task starts it again). Rollback: copy the backup back and restart. Check with
`Get-CimInstance Win32_Process` that the llama-server command line ends with the three flags.

f2-ai caveat (2026-11-07): its 9B started answering only `????` (the backend fails such answers as "unreadable")
some 20-30 minutes after a llama-server start - seen with the new `ai_node.py` without these flags and with the old
`ai_node.py` with them; a node restart clears it for a while. The GPU is shared with the masks service (Hunyuan python), so suspect
VRAM pressure on the 11 GB card, not the flags.

## The film director's brain (worker-4090, 2026-10-11)

`C:\ProgramData\AutoRig\ai-director` is a second node on the owner's card: `ai_node.py` ai-node-20261011b with
`config.json`, `ensure.ps1` and the task `AutoRig 4090 AI Director`.

* It serves `qwen38-27b-uncensored` (Qwen3.8 27B IQ4_XS, our own file).
* llama-server flags: `-c 131072 -fa on -ctk q4_0 -ctv q4_0 -np 1`, text only.
* `max_prompt_chars` is 700000. It listens on 127.0.0.1:5482, with llama on 8094.
* The VPS reaches it through `ssh -R 15420`, with the token in `/srv/autorig/secrets/film-director-brain.token`.
* It needs about 18.9 GB. The owner's desktop keeps ~4 GB of the 24, so q8_0 KV at 128k (~21 GB) does not fit.
* It runs while the owner plays (owner 2026-10-11, `owner_games` empty). cs2 and the desktop leave ~16.5 GB, so it
  loads with `fit_when_short` (`fit_min_vram_mb` 9000, `fit_target_mb` 1536): part of the layers in RAM, still
  128k. The first such load took 61 s.
* Owner priority is on (`owner_priority`, `owner_preempt_comfy`, `owner_warm_seconds` 600). The film director and the
  personalities mark the calls that answer the owner. The first one interrupts the 4090's render and loads the
  model (56 s, measured 2026-10-11); later ones inside the warm window take 4 s. renderfin moved the interrupted job
  to f12 (retry 1/2).
* It writes `gpu.lease`. The 9B pool node `C:\ProgramData\AutoRig\ai-node` lists that file in `gpu_lease_yield_files`
  and steps aside.
* It is not in the backend's AI registry: only the film director (`/srv/autorig/data/red_film`) calls it.

## Several models and parallel tasks (2026-11-06)

`ai-node-20261106-models` adds two config keys; without them a node behaves exactly as before.

**`models`** - a list of model profiles next to the pinned one. Each entry has a `model_id` plus any of
`title, weights, mmproj, context_tokens, max_output_tokens, reasoning, uncensored, system_prompt_supported,
vram_need_mb, extra_llama_args` (its own flags come after the node-wide `extra_llama_args`). A known id
(`bonsai2-27b`, `qwen35-9b-uncensored`, `qwen35-4b`) fills in what is left out. `server-status` lists every
installed profile in `ai_models.models`; a request names one with `model`, no `model` = the primary (top-level
`model_id`, or the first entry). One model is loaded at a time: a task for the other one waits until the
running tasks finish, then the node stops llama-server and loads the other weights (`ai_node.model_swaps`).

**`parallel_tasks`** (1) - tasks generating at the same time. llama-server needs as many slots, so pair it with
`-np <n> -kvu` in `extra_llama_args` (`-kvu`: one KV buffer of `context_tokens` shared by the slots, so a single
long request still gets the whole window and VRAM stays as with one slot). Loads and swaps stay one at a time,
and the node never stops llama-server under another task that is still generating. Decode on these cards is
memory-bound, so two answers at once give more files per minute; one answer alone gets a little slower.

**Do not turn it on yet (2026-11-06).** On f2 (GTX 1080 Ti, llama.cpp b10709, qwen35-9b-uncensored + BF16 mmproj)
two direct vision requests sent next to the node's own running task made llama-server abort
(exit 0xC0000409) after ~12 s; the node's task failed with `model_unreachable` and the node reloaded the model
(36 s). The cause (CUDA OOM with three ~2000-token image prompts, or a multi-sequence bug of this build with
Qwen3.5's hybrid attention) was not caught: `llama_log_file` is rewritten on every start. Reproduce on a box out
of rotation, with the log copied away, before any node runs `parallel_tasks` > 1 or `-np` > 1 with images.

Tests: `python -m unittest test_models_config` (config, routing, swap; no GPU) and `python selftest_parallel.py`
(4 slow fake tasks: parallel 2 finishes in ~9 s instead of ~15 s, one llama-server start), plus `selftest.py`.

### Qwen3.5-4B on a node (A/B of 2026-11-06; the owner installs the model)

* Files: `unsloth/Qwen3.5-4B-GGUF` - `Qwen3.5-4B-Q4_K_M.gguf` (2 740 937 888 bytes) and `mmproj-F16.gguf`
  (672 423 616 bytes), into `<models>\qwen35-4b\` (the projector's name is generic). Public repo, no token
  needed; `HF_TOKEN` is used if present (render-fleet: set on f5, f15, Raptor, worker-4090 - not on f2/f11/f13).
  ```powershell
  huggingface-cli download unsloth/Qwen3.5-4B-GGUF Qwen3.5-4B-Q4_K_M.gguf mmproj-F16.gguf --local-dir C:\ProgramData\AutoRig\ai-node\models\qwen35-4b
  ```
* One command on the box (download, size check, config entry, idle-gated restart):
  ```powershell
  powershell -NoProfile -ExecutionPolicy Bypass -File add-qwen35-4b.ps1 -Task "AutoRig AI Node F13" -Download -NewCode .\ai_node.py
  ```
  f11 keeps its 9B in the converter's read-only `ai-vision\models`: add `-ModelsDir C:\ProgramData\AutoRig\ai-node\models`.
* llama-server flags of the 4B profile, each checked in `llama-server --help` of the b10709 build on f13
  (`version: 0.2.0-dev (build 10709, commit 9a9394a89)`): `-ngl 99` (node `ngl`), `-c 4096` (`context_tokens`),
  `-b 2048 -ub 1024`, `-fa on`, `-ctk q8_0 -ctv q8_0`, `--image-min-tokens 64 --image-max-tokens 1024`,
  `--reasoning off` (from `reasoning: off`). `--chat-template-kwargs '{"enable_thinking":false}'` is supported
  too but left out: `--reasoning off` is the same switch, and answers already come back with an empty
  `reasoning_content` (0 characters on f13).
  Plus Qwen's non-thinking sampling defaults `--presence-penalty 1.5 --top-k 20 --top-p 0.8` (server
  defaults; the request still sends temperature 0.3). Without them the 4B looped one keyword
  ("sensors", "industrial", ...) until the 640-token cap on f2 (2026-11-07); with them 4 of 4 smoke
  answers had no duplicates.
* Backend: `AI_MODEL_EXTRA=qwen35-4b` in `/srv/autorig/secrets/storage-host.env` next to `AI_MODEL_SINGLE`,
  then a restart of autorig-storage (a deploy wipes queues - pick an empty moment). Requests that name
  `qwen35-4b` go only to nodes listing it; every other request is still served by the single 9B.
* A/B: `backend/tools/vision_ab/` (`run_ab.py`, `judge.py`, `summarize.py`).

### Live state 2026-11-06 (see R:\autorig\handoff.md)

f2, f13 ran `a1919c78` (ai-node-20261007); f11 an own build. The throughput patch for that build
(`patches/a1919c78-parallel-tasks.diff`, VERSION `ai-node-20261007-par`) adds only `parallel_tasks`;
`apply-node-tuning.ps1 -Task <task> -NewCode <patched ai_node.py> -Parallel <n> -LlamaArgs <flags>` installs it
(backups `*.bak-vab-20261106`, idle-gated restart, `-Rollback` restores). Not applied anywhere: f13 went into the
GPU swap before the change, and the f2 crash above stopped the rollout.

2026-11-07: the 4B ran on f2 (ai-node-20261106-models) for the A/B, lost on quality (judge 81 % of the 9B) and was
rolled back: f2 is on its original `ai_node.py` and config again (4B files stay in `models\qwen35-4b`). All four
Qwen nodes now carry the sampling flags above; nothing else changed on them.
