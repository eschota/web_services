# AI worker system-role patch

This patch adds one backwards-compatible request field:

```json
{"prompt":"user material","system_prompt":"optional privileged instruction"}
```

An empty or omitted `system_prompt` produces the legacy one-message request.
When present, `BonsaiAdapter` sends an OpenAI-compatible message list with the
system message first and the existing user content second. The two strings keep
the existing combined 8000-character ceiling. The system prompt is deliberately
removed from task status JSON.

`/server-status` advertises the upgrade at
`ai_models.system_prompt_supported: true`. This is a shared adapter capability,
so it applies honestly to both installed text choices (`bonsai2-27b` and
`qwen35-9b-uncensored`) on the upgraded node. Central routing must require this
flag only for requests that actually contain `system_prompt`; legacy requests
remain eligible for old nodes. Each model row carries the semantic canary result:
`bonsai2-27b` is `true`; `qwen35-9b-uncensored` is `false`. Qwen accepts the
message shape but followed the later user instruction in the conflict canary,
so routing must not present it as a verified system-prompt model.
The same fact is available without scanning model rows as
`ai_models.system_prompt_models: ["bonsai2-27b"]`.

The same patch supports an explicit `max_output_tokens: -1` sentinel. Omitted
values keep each model's legacy default; positive values keep their existing
per-model ceiling. Only `-1` bypasses that ceiling and is forwarded unchanged as
llama `max_tokens: -1`, so context length and EOS remain the real bounds. Both
installed model rows advertise `unlimited_output_supported: true`, and completed
task usage records `requested_max_tokens` for canary evidence.

Audited f13 source hashes before the patch:

- `bonsai_adapter.py`: `481e1ca9be4838b2c3fd920d61a021b8e0635bd1828ef1ec6b1e84d775d783b2`
- `webserver_converter_glb.py`: `61b3935d4b09a78e64f415d5f3f8a2e222e6dccdfe3bf7e926913e5dea3ed4c1`

Check without writing:

```powershell
python apply_system_prompt_role.py --root C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer
```

Apply source files atomically, still without restarting:

```powershell
python apply_system_prompt_role.py --root C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer --apply
python preflight_system_prompt_role.py --root C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer
```

Before any restart, query the authenticated `server-status` endpoint and require
both `tasks_summary.processing == 0` and `tasks_summary.queue_size == 0`. Record
the listener PID and source path. Restart through the node's existing converter
controller, then verify the new listener PID is newer than both patched files;
copying files or restarting a scheduled task alone does not prove the serving
process changed. Run a legacy prompt canary and a system-vs-user role canary.

Do not restart a node that is processing conversion, Comfy, Hunyuan or AI work.
