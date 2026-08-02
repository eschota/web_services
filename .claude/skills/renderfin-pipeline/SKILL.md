---
name: renderfin-pipeline
description: Architecture, farm topology, deployment procedure, operational runbook and hard-won gotchas for the autorig.online renderfin 3D character generation pipeline (Telegram "Сгенерировать" button → Flux T-pose render → Hunyuan3D → turntable → full convert pipeline). Use this whenever the work touches renderfin, character generation, the Flux/t_pose workflow or its prompts, the Telegram bot's generation flow, the Hunyuan3D converter workers, the farm boxes f5/f7/f13/f15, the farm SSH tunnels, the disk-pressure cleanup, or the 6-hourly health check — and also when deploying anything in autorig-online/backend, since the production tree does NOT accept a git pull. Read it before debugging a failed generation, changing a render prompt, or touching a farm worker: most of the surprising behaviour in this system is documented here and is expensive to rediscover.
---

# Renderfin 3D generation pipeline

Renderfin turns a Telegram button press into a rigged, animated 3D character.
It is a FastAPI service on the autorig.online VPS that orchestrates GPU boxes on
a remote farm, and it is deliberately built so a job survives anything: service
restarts, farm reboots, expired credentials, an empty worker pool.

**Read `references/gotchas.md` before debugging anything here.** Most of the
surprising behaviour in this system was expensive to discover and is written
down there — a silent Blender exit code, a negative prompt that reaches no
model, a status endpoint that does not check the credential it is given.

## The pipeline

One job moves through these stages (`renderfin/models.py`):

```
flux_render → awaiting_image_approval → hunyuan → turntable → ready → submitted
                                     ↘ discarded          ↘ failed
```

- **flux_render** — two T-pose renders of the *same character* in two styles
  (realistic, low-poly cartoon), queued together so they cost roughly one render
  of wall-clock time.
- **awaiting_image_approval** — the only human decision in the whole flow. The
  operator picks a style; a failed second variant is tolerated.
- **hunyuan** — the chosen alpha-isolated render goes to a converter box's
  Hunyuan3D 2.1 API and comes back a GLB.
- **turntable** — a 6-second orbit video rendered on the VPS with headless
  Chrome + ffmpeg.
- **ready → submitted** — auto-submitted into the full autorig convert pipeline
  (retopology 1k/10k/100k, bake, rig, animations, every format). No button.

The chat shows **one message per job**, rewritten as it moves. Cards whose
moment has passed (the variant choice, a failure that was retried) are deleted.
Cleanup is private-chat only — groups are a shared log nobody asked us to
rewrite.

## Where things live

| What | Where |
|---|---|
| Service | `autorig-renderfin` on 127.0.0.1:8010, nginx `/renderfin/` |
| Code | `autorig-online/backend/renderfin/` |
| Prompt generation | `autorig-online/backend/render_prompting.py` + `render_prompt_instruction.json` |
| Bot side | `autorig-online/backend/telegram_bot.py` |
| State | sqlite `/var/autorig/renderfin/db/renderfin.db`, table `chargen_jobs` (whole job as JSON) |
| Artifacts | `/var/autorig/renderfin/render/<user>/` |
| Worker pool | `/etc/autorig-renderfin-hunyuan.json` (mode 600) |
| Health check | `deploy/healthcheck/renderfin_healthcheck.py`, 6-hourly timer |

## Deploying

**The production tree does not accept `git pull`.** `/root/autorig-online` has
its own parallel git history — the same work committed locally with different
SHAs — so a pull reports divergence and aborts. Deploy by copying the files you
changed and restarting:

```bash
scp backend/renderfin/foo.py autorig-vps:/root/autorig-online/backend/renderfin/
ssh autorig-vps "systemctl restart autorig-renderfin.service"
```

Before copying, confirm the production copy still matches what you started
from, so a prod-only fix is not silently clobbered:

```bash
git show HEAD~1:autorig-online/backend/renderfin/foo.py | sha256sum
ssh autorig-vps "sha256sum /root/autorig-online/backend/renderfin/foo.py"
```

Run the tests on production too — it has dependencies the local machine does
not, and it is where the code will actually run:

```bash
ssh autorig-vps "cd /root/autorig-online/backend && PYTHONPATH=/root/autorig-online/backend /root/autorig-online/venv/bin/pytest tests/test_renderfin_*.py -q"
```

Locally, ten test modules import `main` and need `slowapi`, which is not
installed; ignore them rather than treating them as failures. See
`references/runbook.md` for the exact command.

## Durability: what must never fail a job

The operator pressed a button and is owed the result. Several conditions look
like failures but say nothing about the job, so they **park and wait** instead
of spending an attempt:

- `NoWorkerAvailable` — no box has Hunyuan enabled, or the pool is empty
- `NoWorkerAvailable` from a 401/403 — a box re-provisioned its token
- `TaskVanished` — a box rebooted and forgot a task it had accepted
- A farm-side post-processing breakage (a missing Vertex-PBR manifest)

Jobs already failed by these conditions are revived automatically by the retry
loop, matched on their terminal error text. When adding a new condition, extend
`_FLEET_ERROR_MARKERS` in `renderfin/character_gen.py` so the ones that already
failed come back too.

The cost of this design is silence: a permanently broken condition parks every
job with nobody told. That is why the health check probes worker credentials
directly — see `references/runbook.md`.

## Reference files

Read these when the work touches their area:

- **`references/gotchas.md`** — the non-obvious behaviour of Flux, Blender,
  Telegram, the converter API and the test suite. Read this first when
  debugging; it is the highest-value file here.
- **`references/farm.md`** — box roles, SSH tunnels, tokens, how to park or
  restore a worker, and how to reach a Windows farm box.
- **`references/runbook.md`** — health check, disk-pressure cleanup, token
  refresh, reviving jobs, and the exact test commands.
- **`references/prompts.md`** — how the two render styles are composed, what the
  measurements say about the pose masks, and the prompt anti-patterns that
  produce flat or unusable renders.
