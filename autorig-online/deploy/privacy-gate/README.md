# Privacy gate (private models, owner 2026-10-17)

Spec: `docs/my-gallery-privacy-spec-20261017.md`. Code: `backend/task_privacy.py`.

nginx serves some task files without the backend: `/u/<upload>/…` (sources), `/pw/<task>.webp` (card posters) and
everything under `/api/mt/` (MT run files). For those, nginx asks the gate with `auth_request`; the gate answers
204 (allowed) or 403 (private task, caller is not its owner/admin), and nginx turns the 403 into a plain 404.
The backend's own routes are covered by `task_privacy.PrivacyMiddleware`.

* service: `autorig-privacy-gate.service` (127.0.0.1:8292, user autorig, reads SQLite read-only, no secrets)
  `sudo cp autorig-privacy-gate.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now autorig-privacy-gate`
* nginx: `python3 nginx_patch.py <copy of /etc/nginx/sites-enabled/autorig.online-storage>` writes `<copy>.new`
  (idempotent, exact anchors); install it, `sudo nginx -t && sudo systemctl reload nginx`.
  Backup of the site before the change: `/srv/autorig/data/var/privacy/nginx-site.before-privacy-20261017`.
* health: `curl -s 127.0.0.1:8292/health`
* admin-side switch (support): `cd /srv/autorig/current/autorig-online/backend && sudo -u autorig env
  DATABASE_URL=sqlite+aiosqlite:////srv/autorig/data/db/autorig.db /srv/autorig/venv/bin/python3 task_privacy.py private|public <task_id> <who>`
* audit: `/srv/autorig/data/var/privacy/privacy_audit.jsonl`; pending deletes: `pending_delete.json`.
* exposure matrix (run from outside the VPS): `tools/privacy_exposure.py <task_id> --state public|private|deleted --owner-cookie "anon_id=…"`
