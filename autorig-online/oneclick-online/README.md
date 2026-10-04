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

## Restoration verification, 2026-10-05

Production application release `3595e893`; 12 production unit/regression tests
passed. Streaming a 64 MiB asset used approximately 232 KiB additional Python
memory. A 192-request live read/invalid-input run stayed around 85–88 MiB RSS;
there was no growing spool/cache usage in that run. A real 10 GiB upload session
was admitted using only 258 bytes of metadata, then the explicitly owned probe
was removed. A real HTTPS archive fetch returned 4,761,602 bytes with the expected
SHA-256 and left no partial file. These are bounded tests, not a claim that every
possible long-running workload has been proved leak-free.

The real owned scene was submitted through the production browser. The first
accepted F2 worker job `40cb0323-0663-4d4d-abc9-b0fabe6367b9`, output GUID
`cc4367bf-a48c-492a-acd7-0441b8e3bdfb`, produced Max exports (23 meshes,
3 materials), 15 Max renders, 7 textures, 9 URP renders, a 32.8 MB Unity package,
a 36.2 MB Windows build ZIP and an MP4. Android/Quest/WebGL phases failed because
the required playback engines were absent on the checked nodes. HDRP was still
processing at the last inspection; full cross-platform completion is not verified.
A delayed browser restart linked site task `00d5b630-a5fb-4e5f-9140-2c032ab77dde`
to another queued attempt. Active restarts now return HTTP 409 to prevent an
earlier worker job from becoming untracked.

F7 now has AndroidPlayer, WebGLSupport, JDK 17.0.9+9, NDK r27c, SDK/CMake/build
tools/platforms/cmdline-tools installed from the official version manifest. Temporary
installers were removed. Only the owner-approved `android-sdk-license` was accepted,
verified by the installed sdkmanager. The SDK file anchors pass; Unity batch target
validation stops with `No valid Unity Editor license found`. Do not claim working
Android/Quest/WebGL until the owner activates a valid F7 Unity license and the
actual target builds pass. Current OneClick environment restricts new dispatch to
the known working F2; the rest of AutoRig's pool is independent.

Runtime receipts and screenshots remain under `R:/autorig/.work/oneclick-discovery`.
The installed SDK consent receipt supersedes the earlier raw-XML license hash.
The correct sdkmanager-derived accepted hash is
`24333f8a63b6825ea9c5514f83c2829b004d1fee`.

Final application release: `3d183437`. The initial worker canary is now natively
`Completed`; its actual 53-file inventory is preserved on the site at
`/task?id=40cb0323-0663-4d4d-abc9-b0fabe6367b9`. Max, URP and HDRP/Windows outputs
exist. This does not verify Android/Quest/WebGL, which failed on F2 due to missing
modules. The separate restarted attempt remains in the normal worker queue.

All 53 discovered files were downloaded through the existing worker SSH tunnel
with zero cache errors: 154,482,339 cached bytes. Repeated catalog access took
6.6 ms and did not grow the cache. URP/HDRP `cam1.png` have distinct URL-derived
cache keys and distinct SHA-256 values; both preview HTTP responses match their
own cached bytes. The Windows URP ZIP (36,244,852 bytes, 164 entries) and HDRP ZIP
(68,949,149 bytes, 163 entries) pass CRC validation. Thirteen regression tests pass.
Filesystem page cache contributes to cgroup memory; warmed-process RSS was about
137 MiB, below the configured limits. This is a measured finite run, not an
absolute guarantee against every future memory/disk leak.

Production cleanup removed approximately 719 MB of owned release/staging files
without deleting task originals, the current release or a rollback release.
Automatic tool policy rejected deletion of four local staging archives; they
remain inside the repository-owned `.work/oneclick-discovery` directory.
