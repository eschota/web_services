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

## Shared level documents

Levels authored in the game's own editor (Windows player and WebGL) are stored by the
same service under `data/levels/`. A record is `{schemaVersion, levelId, revision, updatedAt, document}`;
the document is the game's `little-tilt.level` JSON (format/version/levelId/title/ivy/layers).
The service only bounds shape and size (1 MB, 1000 plants, 64 layers); the game validates plant
recipes on load. Two namespaces with different write policies:

- **Official level** `data/levels/<levelId>.json` — what every player loads in Game mode and what
  the game repository tracks in git. Writable only with the developer publish token.
  Owner decision (2026-09-17): public visitors may not change the level other players see.
- **Player drafts** `data/levels/drafts/<levelId>/<draftId>.json` — public, one per editor session
  (`draftId` is a client-generated id), autosaved from the in-game editor, never read by Game mode
  and never promoted automatically. At most 200 drafts per level; the oldest are evicted.

Routes (all JSON, `Cache-Control: no-store`):

- GET `/gravityhouse/api/levels` → `{levels:[{levelId, revision, updatedAt, title, plants, bytes}], publishing}`.
- GET `/gravityhouse/api/levels/<levelId>` → official record (404 until first publish).
- POST `/gravityhouse/api/levels/<levelId>` with header `X-Level-Token: <token>`, JSON
  `{revision, document}` → publishes; first publish uses `revision: 0`; stale revision → 409;
  missing/wrong token → 403. Each publish keeps the replaced record in `data/levels/history/`
  (last 20): GET `.../history` lists them, GET `.../history/<revision>` returns one.
- GET `/gravityhouse/api/levels/<levelId>/drafts` → `{drafts:[{draftId, author, revision, updatedAt, plants, bytes}]}`.
- GET/POST `/gravityhouse/api/levels/<levelId>/drafts/<draftId>` → read / save a draft
  (`{revision, document, author?}`, same revision rule, no token).

Publish token: the service reads `LEVEL_WRITE_TOKEN` from the environment or, if unset, the file
`data/level-write-token` (installed by `install.sh` from `incoming/level-write-token`, mode 0600,
owner www-data). Without a token, publishing is disabled (`publishing: false`) and the game only
reads. Keep the same token on the developer machine in the game repository's
`.runtime/level-write-token` (git-ignored) for `Tools/sync_levels.py push` and the Windows
developer build; never ship it inside a WebGL release.

Level routes share the graphics CORS allowlist (site plus local development origins) and the
per-IP rate limit. `nginx.conf` raises `client_max_body_size` to 2m for `/gravityhouse/api/`.
The game repository mirrors official levels in `Assets/Resources/Levels/` (see the game's
`Tools/sync_levels.py`); a new WebGL release never overwrites `data/levels`.

Tests: `python test_store.py`. Temporary test data stays under `.test-tmp`
within this directory. Tests cover persistence, reset, stale writes, invalid values and level documents.
