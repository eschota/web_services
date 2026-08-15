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

Keep secrets in `/srv/autorig/secrets`. The checked-in `storage-host.env` is a
non-secret overlay; copy its values into `/srv/autorig/secrets/storage-host.env`
without replacing migrated credentials. F7 remains excluded by
`AUTORIG_DISABLED_WORKERS` until the worker has been separately repaired and
validated.
