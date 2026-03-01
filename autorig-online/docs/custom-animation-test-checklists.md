# Custom Animations Test Checklists

## Scope

This checklist validates the custom animation release on `task` page:

- Catalog from `static/all_animations/*.json` and `manifest.json`
- Per-animation purchase (`1 credit`)
- Unlock-all purchase (`10 credits`)
- Selected animation download in `FBX` format only
- Viewer selection flow (type filter, card selection, playback)

## Unit Checks

- [ ] Each `.glb` in `static/all_animations` has a paired `.json`
- [ ] `manifest.json` has unique `id` values
- [ ] `pricing.single_animation_credits == 1`
- [ ] `pricing.all_animations_credits == 10`
- [ ] `pricing.download_format == "fbx"`
- [ ] All manifest items use `credits = 1` and `format = "fbx"`

Run:

```bash
python -m unittest /root/autorig-online/backend/tests/test_custom_animation_metadata.py
```

## Integration Checks (Backend + DB)

- [ ] Catalog endpoint returns pricing and grouped types
- [ ] Catalog marks animation `available=true` when matching task FBX exists
- [ ] Single purchase deducts exactly `1` credit
- [ ] Duplicate single purchase is idempotent (no second deduction)
- [ ] Unlock all deducts exactly `10` credits
- [ ] Legacy `/purchases` all-flow deducts `10` credits
- [ ] Task owner receives mirrored credit rewards (except self-purchase)

Run (in project venv with backend deps):

```bash
/root/.venv-autorig/bin/python -m unittest /root/autorig-online/backend/tests/test_custom_animation_billing.py
```

## E2E UI Checks (Task Page)

- [ ] Open done task page and verify `Custom Animations` block appears
- [ ] Type dropdown filters cards (`walk`, `run`, `jump`, etc.)
- [ ] Clicking card highlights selection and tries to play clip in viewer
- [ ] Buy selected button charges `1` credit and unlocks download
- [ ] Unlock all button charges `10` credits and unlocks all card downloads
- [ ] Download selected uses `/api/task/{task_id}/animations/download/{animation_id}`
- [ ] Section stays hidden for non-`done` task status

Quick smoke:

```bash
/root/.venv-autorig/bin/python - <<'PY'
import httpx
task_id='84ff30bd-612a-481d-9606-0045fe6aed15'
r=httpx.get(f'http://127.0.0.1:8000/api/task/{task_id}/animations/catalog', timeout=20)
print(r.status_code, r.json().get('pricing'))
PY
```

## Load & Stability Checks

- [ ] Catalog endpoint p95 latency < 250ms at 50 RPS
- [ ] Purchase endpoints remain idempotent under concurrent repeated clicks
- [ ] No duplicate credit deductions during retries/timeouts
- [ ] Download endpoint keeps stable throughput for concurrent FBX downloads
- [ ] Error rate (`4xx/5xx`) stays within baseline range during load

Suggested load matrix:

- `GET /api/task/{id}/animations/catalog` at 10/25/50 RPS
- `POST /api/task/{id}/animations/purchase` burst (double-click simulation)
- `GET /api/task/{id}/animations/download/{animation_id}` concurrent 5/10/20

## Exit Criteria

- All unit + integration tests pass
- E2E checklist has no blockers
- No critical billing/security defects
- Canary metrics stay green for 24-48h
