# Custom Animations UAT Report

Date: 2026-03-01  
Environment: `http://127.0.0.1:8000` (service `autorig.service`)  
Sample task: `84ff30bd-612a-481d-9606-0045fe6aed15`

## Regression + UAT Smoke Results

- `GET /task?id=<task_id>` -> **200** (task page renders, downloads section present)
- `GET /api/task/<task_id>` -> **200** (task status payload valid)
- `GET /api/task/<task_id>/purchases` -> **200** (legacy purchase state intact)
- `GET /api/history?per_page=5` -> **200**
- `GET /api/gallery?per_page=5&sort=likes` -> **200**
- `GET /api/queue/status` -> **200**
- `GET /api/task/<task_id>/animations/catalog` -> **200** (new custom animation catalog endpoint)
- `GET /api/task/<task_id>/animations/download/walking` -> **401** without auth (expected access protection)

## Billing Validation (Automated)

- Single animation purchase: **1 credit**
- Duplicate same animation purchase: **idempotent**, no second debit
- Unlock all custom animations: **10 credits**
- Legacy `download all` flow (`/api/task/{id}/purchases` with `all=true`): **10 credits**
- Owner credit accrual validated in integration test

Executed test modules:

- `backend/tests/test_custom_animation_metadata.py`
- `backend/tests/test_custom_animation_billing.py`

## Conclusion

Pre-prod UAT smoke and regression checks passed for existing task flow plus new custom-animation APIs.
