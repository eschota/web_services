# Gravity House WebGL

Public URL: https://autorig.online/gravityhouse/

The infrastructure map and live SSH inspection identify `way-fr`
(`way.qwertystock.com`) as the production host. Do not deploy to the old AutoRig VPS.

Game source: `R:\AssetStore\GravityHouseMVP`, Unity 6000.3.21f1.
Build with `Tools/Build-WebGL.ps1`, commit source, then run
`python Tools/package_webgl.py` in that project. The result and SHA-256 are in
`QA/webgl_release.json`; build payloads stay outside this repository.

Remote project root: `/srv/autorig/webgl-assets/gravityhouse`.
Upload the release archive, `nginx.conf` and `deploy.sh` to its `incoming` folder.
Run `sudo -n bash incoming/deploy.sh RELEASE ARCHIVE_SHA256` using absolute paths.
The script checks the archive and per-file manifest, preserves old hashed Build
files, validates nginx, atomically switches `current`, reloads nginx and verifies
the actual served release. Backend services are not restarted.

The existing HTTPS site gets one include for
`/etc/nginx/snippets/autorig-gravityhouse-webgl.conf`. Deploy only this include
and snippet; do not replace the whole live site with a potentially older template.
Parent security headers remain inherited because the location adds no headers.
Gzip is negotiated by `gzip_static`; original filenames retain correct MIME types.

Rollback: read `deploy/RELEASE/previous-release.txt`; atomically repoint `current`
to that retained directory. Hashed assets stay available under `shared`.
For a first-publication rollback, remove only the Gravity House include and reload
nginx after `nginx -t`; the backup site configuration is retained for inspection.

Acceptance: public page + four Build assets return 200, wasm has
`application/wasm` and gzip negotiation, all three levels and both selector groups
change Unity's acknowledged state, pointer drag changes orientation, releasing
the pointer stops rotation, reset produces the portal sequence, no WebGL errors.
Also check `/` and `/gallery` and confirm the backend PID/cwd remain unchanged.
