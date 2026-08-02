# Runbook

## Health check

```bash
ssh autorig-vps "cd /root/autorig-online/backend && \
  /root/autorig-online/venv/bin/python3 /root/autorig-online/deploy/healthcheck/renderfin_healthcheck.py"
```

Runs on a 6-hourly timer; `--notify` posts the digest to Telegram when there is
at least one problem. It checks the site, the four systemd units, disk, the
Hunyuan pool, the render queue, job stages and every tunnel.

Two of its checks exist because their absence hid a real outage:

- **Worker usability** is judged the same way renderfin judges it, and then the
  credential is probed against `generate-3d` — `server-status` answers 200
  whatever token it is given, so both boxes once looked healthy while nothing
  could be submitted.
- **Stall detection** measures from `stage_started_at`, not `updated_at`. A
  service restart refreshes `updated_at`, so a job stuck for a day read as
  minutes old.

## Tests

Locally, ten modules import `main` and need `slowapi`, which is not installed:

```bash
cd autorig-online/backend && python -m pytest tests/ -q \
  --ignore=tests/test_animation_fitting_plan_trust_resolver.py \
  --ignore=tests/test_model_sale_offers.py \
  --ignore=tests/test_paired_rig_source_http_hardening.py \
  --ignore=tests/test_rig_source_transfer.py \
  --ignore=tests/test_task_bundle_downloads.py \
  --ignore=tests/test_viewer_artifact_contract.py \
  --ignore=tests/test_viewer_artifact_hardening.py \
  --ignore=tests/test_viewer_artifact_reconciliation.py \
  --ignore=tests/test_viewer_artifact_review_regressions.py \
  --ignore=tests/test_custom_animation_billing.py
```

On production, everything runs, but `PYTHONPATH` is required:

```bash
ssh autorig-vps "cd /root/autorig-online/backend && \
  PYTHONPATH=/root/autorig-online/backend \
  /root/autorig-online/venv/bin/pytest tests/test_renderfin_*.py -q"
```

## Refreshing a Hunyuan token

A box re-provisions its token on restart. Symptom: `generate-3d` returns
`401 {"error":"unauthorized"}` while `server-status` looks fine.

Read the live token off the box and write it into the pool without printing it:

```bash
TOKEN=$(ssh farm-f13 "powershell -NoProfile -Command \
  \"(Get-Content \$env:LOCALAPPDATA\\AutoRig\\hunyuan_api_token -Raw).Trim()\"" | tr -d '\r\n ')
[ ${#TOKEN} -lt 20 ] && { echo "token read failed"; exit 1; }

printf '%s' "$TOKEN" | ssh autorig-vps "cat > /tmp/.hy && chmod 600 /tmp/.hy && \
  /root/autorig-online/venv/bin/python3 - <<'PY'
import json, shutil, time
p = '/etc/autorig-renderfin-hunyuan.json'
shutil.copy(p, p + '.bak.' + str(int(time.time())))
d = json.load(open(p)); token = open('/tmp/.hy').read().strip()
for w in (d.get('workers') if isinstance(d, dict) else d):
    w['token'] = token
json.dump(d, open(p, 'w'), indent=1)
PY
rm -f /tmp/.hy; chmod 600 /etc/autorig-renderfin-hunyuan.json"

ssh autorig-vps "systemctl restart autorig-renderfin.service"
```

Verify by POSTing an invalid body — a `400 invalid_request` proves the bearer
was accepted:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{}' http://127.0.0.1:15267/api-converter-glb/generate-3d
```

Jobs failed on the stale token revive by themselves once it is fixed.

## Reviving jobs

The retry loop revives jobs whose terminal error matches
`_FLEET_ERROR_MARKERS` — an empty pool, a rejected token, a lost task, a farm
post-processing breakage. Nothing to do by hand: restart renderfin and watch.

```bash
ssh autorig-vps "journalctl -u autorig-renderfin --since '2 min ago' --no-pager | grep -i reviving | wc -l"
```

Job stages straight from the database:

```bash
ssh autorig-vps "/root/autorig-online/venv/bin/python3 -c \"
import json,sqlite3,collections
db=sqlite3.connect('file:/var/autorig/renderfin/db/renderfin.db?mode=ro',uri=True)
c=collections.Counter(json.loads(p)['stage'] for (p,) in db.execute('SELECT payload FROM chargen_jobs'))
print(dict(c))\""
```

## Disk-pressure cleanup

`backend/scripts/run_disk_pressure_cleanup.py`, timer every 15 minutes. It only
acts under pressure, and it deletes a GLB cache entry **only after its upstream
URL answers 200** — the cache is frequently the last surviving copy of a user's
deliverable.

Verdicts are remembered in `/var/autorig/glb_cache_last_copy.json` with a
one-week expiry. Without that memo the fixed probe budget went to the same
permanently-dead entries every run and the pass freed nothing.

To see what a run did, read the JSON line it logs — `prepass_glb_deleted` and
`freed_gb` are the ones that matter:

```bash
ssh autorig-vps "systemctl start autorig-disk-pressure-cleanup.service && sleep 45 && \
  journalctl -u autorig-disk-pressure-cleanup --since '50 sec ago' --no-pager | grep -iE 'Prepass|OVER CAP' | tail -5"
```

`OVER CAP with nothing safe to delete` means everything left is a last copy —
the cap cannot be honoured without losing data, and that is the correct outcome.

## Services

| Unit | What it does |
|---|---|
| `autorig` | main site backend |
| `autorig-renderfin` | this pipeline + Telegram delivery |
| `autorig-telegram` | bot: buttons, auto-submit loop, notifications |
| `autorig-farm-tunnels` | SSH tunnels to the farm |
| `autorig-disk-pressure-cleanup.timer` | 15-minute cleanup |

Restart order after a deploy touching both: renderfin first, then the bot.
