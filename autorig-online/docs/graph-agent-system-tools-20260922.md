# Graph assistant: system role and validated editing tools

Deployed in immutable VPS release `ai-defaults-20260922-t`.

## What changed

The original editor already provided eight JSON editing operations and an
in-place canvas bridge, but its instruction string was concatenated with graph
data into one user message. The text API now accepts optional `system_prompt`,
keeps the combined 8000-character bound, and forwards system/user content
separately through the farm queue to the inference server's actual message
roles. Existing callers without that field retain their previous behavior.

The tools remain a structured JSON operation protocol, not native provider
`tool_calls`: `add_node`, `remove_node`, `update_params`, `set_input`, `connect`,
`disconnect`, `move_node`, and `rename_graph`. Server validation checks the
complete proposed graph before the bridge changes the live canvas. Concurrent
user edits invalidate an outdated AI proposal. Rendering is a separate action.

Live QA uncovered another defect: numeric strings from browser selects or
model output were compared with numeric option values and rejected. The graph
validator now normalizes declared numeric values on a copy before applying
operations. Bounds, finite-number checks, model compatibility, port types and
cycle rejection remain enforced.

## Verified worker scope

Only idle f13 was updated/restarted. Its canonical source is
`C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer`, with final verified PID
8712. The worker advertises `system_prompt_supported: true` and
`system_prompt_models: ["bonsai2-27b"]`. The central dispatcher requires that
per-model capability whenever a request includes system instructions.

Bonsai passed both a legacy-request canary and a conflicting system/user
instruction canary. Qwen transported the real role but did not pass the latter
adherence check, so its graph-editor option is visibly disabled pending
verification. Ordinary Qwen text/vision requests and other farm nodes retain
their previous behavior. This is a bounded conformance test, not a universal
claim about either model's instruction following.

Worker rollout helpers and exact evidence are under
`deploy/ai-workers/system_prompt_role/`.

## Production browser proof

The real Bonsai graph assistant was asked to resize an existing Image node,
add another Image and connect it to the same text source. It applied three
operations without reloading the page and saved
[this graph](https://autorig.online/nodes?g=5d45b98be2d2).

The persisted graph was independently fetched and checked:

- 3 nodes and 2 links;
- existing Image: 832×1216;
- new Image: 960×540;
- both Image prompt inputs connected to the original Text input;
- original link retained;
- no render tasks created by the edit.

The client displays `8 graph tools` and `Graph updated · 3 operation(s) applied`.
Validation: 73 backend tests and 8 frontend tests passed. Production source
hashes were compared against the pre-change baseline before the overlay was
activated; the backend PID/cwd was verified against the new release.
