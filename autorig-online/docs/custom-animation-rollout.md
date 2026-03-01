# Custom Animations Canary & Rollout

Date: 2026-03-01

## Deployment Performed

- Backend code updated in `backend/main.py`, `backend/database.py`, `backend/models.py`
- Static task page updated in:
  - `static/task.html`
  - `/var/www/autorig/static/task.html`
- Animation metadata generated and synced:
  - `static/all_animations/*.json`
  - `static/all_animations/manifest.json`
  - `/var/www/autorig/static/all_animations/*.json`
- Service restarted:
  - `systemctl restart autorig.service`

## Canary Scope

Initial canary checks were run against production domain (`autorig.online`) for:

- Task page rendering with custom animation block
- New catalog endpoint with pricing rules
- Existing history and gallery endpoints (regression)

Results:

- `GET /task?id=<task_id>` -> 200, custom block present
- `GET /api/task/<task_id>/animations/catalog` -> 200, pricing `1/10` confirmed
- `GET /api/history?per_page=3` -> 200
- `GET /api/gallery?per_page=3&sort=likes` -> 200

## Monitoring During Canary

Primary metrics/signals:

- API error rate for:
  - `/api/task/{id}/animations/catalog`
  - `/api/task/{id}/animations/purchase`
  - `/api/task/{id}/animations/download/{animation_id}`
- Credit deduction correctness:
  - single = 1
  - all = 10
  - no duplicate deductions
- Task page client stability:
  - no JS syntax/runtime errors in `task.html`
- Service health:
  - `journalctl -u autorig.service` without crash loops/exceptions

Observed post-deploy logs show normal request handling and no startup/runtime failures.

## Rollback Criteria

Immediate rollback is required if any of the following appears:

- Incorrect credit deductions (double charge, wrong amount)
- Mass 5xx/4xx increase on custom animation endpoints
- Task page breakage (viewer, downloads, or major JS failures)
- Inability to download already purchased animation

## Rollback Steps

1. Restore previous `task.html` in `/var/www/autorig/static/task.html`
2. Revert backend files to previous revision (`main.py`, `database.py`, `models.py`)
3. Restart backend:
   - `systemctl restart autorig.service`
4. Re-run smoke checks for:
   - `/task?id=...`
   - `/api/task/{id}`
   - `/api/history`
   - `/api/gallery`

## Full Rollout Decision

Canary smoke and regression checks are green.  
Proceed with full rollout while keeping billing and endpoint monitoring active for 24-48 hours.
