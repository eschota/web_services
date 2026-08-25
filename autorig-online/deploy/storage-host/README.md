# AutoRig storage host

This directory contains the non-secret service, nginx, and environment overlays
for the dedicated AutoRig deployment under `/srv/autorig`.

## Host prerequisites

Install the system packages used by the migrated TLS configuration and verify
renewal before DNS cutover:

```bash
sudo apt-get install -y python3-certbot-nginx
sudo nginx -t
sudo certbot renew --cert-name autorig.online --dry-run \
  --no-random-sleep-on-renew --non-interactive
```

The AutoRig backend and Renderfin must bind only to `127.0.0.1:8200` and
`127.0.0.1:8210`. Existing analytics listeners on 8000/8011 are not part of this
deployment and must not be changed.

## Release deployment

Deploy an immutable directory named after the published commit SHA under
`/srv/autorig/releases`, verify the exact transferred file hashes and tests,
then atomically repoint `/srv/autorig/current`. Never run a blind `git pull` on
the production host.

Before switching `current`, recreate the two runtime mount targets that are
intentionally absent from a Git archive.  The backend systemd namespace will
fail closed with `status=226/NAMESPACE` if either path is missing:

```bash
release=/srv/autorig/releases/<published-commit-sha>
ln -s /srv/autorig/data/static/tasks "$release/autorig-online/static/tasks"
ln -s /srv/autorig/data/static/glb_cache "$release/autorig-online/static/glb_cache"
test "$(readlink -f "$release/autorig-online/static/tasks")" = /srv/autorig/data/static/tasks
test "$(readlink -f "$release/autorig-online/static/glb_cache")" = /srv/autorig/data/static/glb_cache
```

Create these links in the staged release, never in the shared data directory,
and verify them before restarting `autorig-storage.service`.

Keep secrets in `/srv/autorig/secrets`. The checked-in `storage-host.env` is a
non-secret overlay; copy its values into `/srv/autorig/secrets/storage-host.env`
without replacing migrated credentials. F7 remains excluded by
`AUTORIG_DISABLED_WORKERS` until the worker has been separately repaired and
validated.

After cutover, install and enable the storage-host-specific cleanup and health
timers from this directory. Disable the legacy VPS timers only after both new
ones pass a manual run; otherwise both hosts can emit alerts with unrelated disk
sizes. The health check is parameterized through `AUTORIG_HEALTHCHECK_*` so it
checks ports 8200/8210, `/srv/autorig/data`, and the `autorig-storage-*` units.
Install `nginx-home-logrotate.conf` as `/etc/logrotate.d/nginx-home` because the
global nginx logs live under `/home/log/nginx` and are not covered by Debian's
default `/var/log/nginx/*.log` rule.

For the first three days after a storage/backend migration, also install and
enable `autorig-storage-postmigration-monitor.timer`. It runs every ten minutes,
persists its cursor and audit log under `/var/lib/autorig-postmigration-monitor`, performs a
full farm/credential check hourly, and sends an end-to-end completion-email
probe to Resend's non-user test sink every twelve hours. New journal errors,
task failures, cache stalls, mail-ledger failures, and provider delivery failures
are deduplicated before Telegram notification. Once the 72-hour window closes,
the next scheduled pass writes `postmigration-72h.complete` and disables the
timer.
### Shared GPU workload broker

The broker API is disabled by default. Generate four independent bearer
credentials outside the repository and store them in
`/srv/autorig/secrets/backend.env` as
`AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN`,
`AUTORIG_WORKLOAD_BROKER_RENDERFIN_TOKEN`,
`AUTORIG_WORKLOAD_BROKER_HOST_AGENT_TOKEN`, and
`AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN`. Reusing one value across scopes makes
the broker fail closed. Roll out broker-aware converter and Freestock node
payloads first; only then set
`AUTORIG_WORKLOAD_BROKER_ENABLED=1` in
`/srv/autorig/secrets/feature-flags.env`.  Never pass the token in a process
argument or commit it. Keep the Gateway-side scoped values in its protected
service environment as `FREESTOCK_AUTORIG_WORKLOAD_BROKER_GATEWAY_TOKEN` and
`FREESTOCK_AUTORIG_WORKLOAD_BROKER_ADMIN_TOKEN`; Renderfin may reuse the
backend's Renderfin-scoped value. `storage-host.env` keeps the non-secret two-slot
AutoRig reserve and the F7/Raptor physical-resource alias.
