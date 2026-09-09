# Shared game settings

Owner decision: anyone with the game link may save the shared settings.
The service exposes only validated, bounded control values; no AutoRig account,
task or filesystem operations are available through it.

One-time installation on `way-fr`: copy `server.py`, `schema.json`, `test_store.py`,
`gravityhouse-settings.service`, `install.sh` and the parent `nginx.conf` into
`/srv/autorig/webgl-assets/gravityhouse/incoming`, then run
`sudo -n bash /srv/autorig/webgl-assets/gravityhouse/incoming/install.sh`.
The service listens only on 127.0.0.1:8263. The existing AutoRig backend is untouched.

- Public read: `/gravityhouse/settings.json`.
- Field schema/defaults: `/gravityhouse/settings-schema.json`.
- Save: POST `/gravityhouse/api/settings`, JSON `{revision, settings}`.
- Reset and persist defaults: POST `/gravityhouse/api/settings/reset`, JSON `{revision}`.
- Health: GET `/gravityhouse/api/health`.

Persistent data: `/srv/autorig/webgl-assets/gravityhouse/data/settings.json`.
It is outside `current` and `releases` and is created only if missing.
`previous.json` retains one previous revision. Writes use a file lock, fsync,
atomic replacement and revision conflict checks. A stale write returns 409.
Requests accept only the schema's numeric/boolean fields, with no arbitrary paths.

Game builds always fetch this JSON with cache disabled before starting physics.
Changing a WebGL release never installs or overwrites `settings.json`.
Defaults can be changed deliberately through the schema/service; persisted custom
values are still preserved on service startup.

Tests: `python test_store.py`. Temporary test data stays under `.test-tmp`
within this directory. Tests cover persistence, reset, stale writes and invalid values.
