# OneClick3D on WAY

Independent site: https://oneclick3d.xyz (www redirects to the canonical hostname).
Recovered from `eschota/oneclick_server`, path `oneclick-online`, upstream commit
`4e87b9d2931902a1a78f2961a3d6de235c36cab9`. Runtime/environment/database contents
from the old repository are deliberately not part of this source snapshot.

## Production layout

- Unit: `oneclick.service`, user `oneclick`, loopback port 8244.
- Source releases: `/srv/oneclick/releases/<commit>`, `/srv/oneclick/current`.
- Isolated venv: `/srv/oneclick/venv`.
- SQLite WAL, original ZIPs, partial uploads, cache and QA receipts: `/srv/oneclick/data`.
- Credentials: `/srv/oneclick/secrets`; none are stored in Git.
- Nginx source: `deploy/nginx.conf`; log rotation: `deploy/logrotate.conf`.
- AutoRig issues a 120-second HMAC identity grant after its normal Google login.
  OneClick checks cookie nonce, signature, issuer, audience and expiry, then creates
  its own session. No Google tokens or AutoRig sessions are copied into OneClick.
- Certbot uses `deploy/acme_hook.py` through WAY's Namecheap API. DNS changes use
  preview, fresh `zone_hash`, merge and backup. TXT verification records are preserved.

## Queue and storage contract

The database and dispatcher are separate. F2/F7/F11/F13 share their existing
converter queues and resource guards with AutoRig. F1 is excluded from this
provider because its required OCConvert exporter source/base files were absent
at restoration. Other workers' exporter hashes were identical and inspected
read-only; no farm runtime was overwritten.

Worker requests carry the real backend UUID and interactive workload class.
Uploaded ZIPs are served through `/oneclick-input/` on AutoRig's existing trusted
download hostname using `deploy/autorig-input.conf`. This is a streaming facade
to the OneClick upload endpoint; it does not duplicate storage or combine databases.

Defaults: 10 GiB maximum archive, 24 GiB original/unfinished upload budget,
8 GiB recoverable cache, 2 GiB per cached asset, 20 GiB free disk reserve. Chunk
sessions reserve space for parts plus the final merged archive. The server refuses
new work when these budgets cannot be met. Completed originals and the database
are protected; capacity does not imply unlimited archival retention.

Downloads use 64 KiB buffers, atomic temporary files, bounded concurrency and
fixed lock pools. Image decoders have a four-million-pixel limit. Metadata responses
are bounded. Only recoverable cache is evicted under quota/free-space pressure;
unfinished uploads expire after their inactivity limit. Active task artifacts are
protected. Cleanup removes partial cache files and empty cache directories, and
prunes upload bookkeeping. Init requests are rate limited with at most 128 open sessions.
Systemd bounds memory at 900 MiB (high watermark 600 MiB), one Uvicorn worker and
24 concurrent connections. Nginx streams request/response bodies without a second
disk buffer. Auth callbacks are excluded from access logs.

## Release and verification

Commit/push source before deployment. Stage only this provider directory beside
the current release, verify archive hash, readable files, syntax/import and tests,
then atomically replace `current`; restart only `oneclick.service`. Preserve the
previous release for rollback. Any AutoRig bridge change is patched at the exact
`app.include_router(build_model_sale_router(get_current_user))` anchor inside a
copy made with `cp -a`; never replace its entire branch or lose ownership/ACLs.

Health: `/health`, `/health/storage`; live queue: `/api/queue/status`.
Tests: run `backend/test_storage.py` with OneClick's production venv and project-owned
test directories. Real restoration scene is the converter-owned `3dmax/base_scene.max`
(SHA-256 `af812652e3e364aa505a845958090d1f3f1af7a9d9b7ff413c84bcb1f240a6d0`)
plus its checked-in textures, not any customer's uploaded file.

The old VPS database/secrets were not recovered. Existing OneClick email/Telegram
notifications and payment-provider callbacks require their own verified credentials
and configuration; the common Google login does not grant access to AutoRig's
payment credentials or its separate YouTube integrations.
