# web_services Project Rules

This repository is the local working copy for:

```text
R:\autorig
```

Remote repository:

```text
https://github.com/eschota/web_services.git
```

The production VPS checkout uses the same repository over SSH:

```text
git@github.com:eschota/web_services.git
```

Production SSH access is configured in the user's `~/.ssh/config` as:

```bash
ssh autorig-vps
```

That alias reaches `autorig.online` itself (37.187.57.177, `way.qwertystock.com`,
SSH on port 22744, user `debian`, passwordless `sudo`). It used to point at
185.171.83.65; that box is still up but serves no web traffic, and is now
`autorig-vps-legacy`.

`autorig.online` is deployed as immutable releases, not as a checkout that is
pulled in place:

```text
/srv/autorig/current -> /srv/autorig/releases/<commit sha>
```

* backend: `autorig-storage.service`, uvicorn on `127.0.0.1:8200`, working
  directory `/srv/autorig/current/autorig-online/backend`
* static: nginx serves `/srv/autorig/current/autorig-online/static`
* secrets: `/srv/autorig/secrets/` (not in Git)

Deploy by staging a new directory under `/srv/autorig/releases`, verifying it,
then repointing `current` and restarting the service. Never `git pull` inside a
release. `/root` is the old VPS layout and does not apply to this host.

Use this `AGENTS.md` as the project rule source. Do not create or rely on
Cursor `.cursor/rules` instructions for this project.

## General Workflow

Use a production-first deployment workflow for AutoRig.online. Do not run local
dev servers, local preview servers, or local browser QA for this project unless
the user explicitly asks for a local-only experiment. The local checkout exists
for source control, commits, and exact file preparation; runtime verification
happens on production.

1. Make changes in `R:\autorig`.
2. Check the local working tree before editing:

```bash
git status --short
```

3. Keep changes scoped to the requested service directory.
4. Commit and push from the local repository when the change should be
   preserved.
5. Deploy immediately to production and test against `https://autorig.online`
   with `curl`, service logs, and browser checks.
6. Fix production-visible issues in the same deploy loop. If a direct production
   edit is needed to unblock the site, mirror the exact change back into
   `R:\autorig`, commit it, and push it.
7. Deploy `autorig.online` as a new release, never by pulling in place:

```bash
ssh autorig-vps
CUR=$(readlink -f /srv/autorig/current)
sudo cp -a "$CUR" "$CUR-<change>"          # stage beside the live release
# copy in the exact changed files, run the relevant tests inside the staging dir
sudo ln -sfn "$CUR-<change>" /srv/autorig/current.new
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
sudo systemctl restart autorig-storage.service
```

The deployed tree can be ahead of a local branch, so never ship a branch
wholesale to fix one thing: carry only the changed files, and patch a shared
file such as `main.py` at exact anchors. Rolling back is repointing `current`
at the previous release and restarting.

Avoid SSH-only code edits. If an emergency production edit is unavoidable, copy
the exact change back to `R:\autorig`, commit it, and push it so local git
and production do not drift.

Never commit runtime caches, browser state, virtualenvs, build artifacts,
secrets, logs, uploaded files, or generated backup files unless the task
explicitly requires it.

## Repository Layout

Important top-level service directories:

```text
autorig-online/          AutoRig.online production web app
autorig/                 AutoRig related assets/tools
qwerty_vpn/              QwertyStock VPN gateway/proxy service
CGTrader_SUBMIT_SERVER/  CGTrader submit server
```

There are also historical/runtime-looking directories such as `.config`,
`.local`, `.venv-autorig`, `.vscode-remote-containers`, `.wdm`, and `opt`.
Treat these as sensitive or runtime state unless the user gives a specific task
for them.

Virtualenv directories are runtime state, not source code. Keep them ignored:

```text
autorig-online/venv/
autorig-online/mcp/.venv/
qwerty_vpn/gateway/venv/
CGTrader_SUBMIT_SERVER/venv/
```

If a cleanup commit removes tracked venv files, do not deploy it with a plain
`git pull` over production unless the production venvs have first been copied
aside or recreated. Safe sequence: copy the venv directory outside the repo,
pull the cleanup commit, move the venv back into the same ignored path, then
verify the matching service. `autorig.service`, `autorig-telegram.service`,
`qwerty-gateway.service`, the AutoRig MCP process, and
`cgtrader_submit.service` have been observed using in-tree venv paths on the
VPS.

## AutoRig.online

Production application path (through the release symlink):

```text
/srv/autorig/current/autorig-online
```

Do not use `/opt/autorig-online` as production. Do not deploy from `/opt`.
There may also be a small `/autorig-online` directory at filesystem root; it is
not the current backend checkout.

Active production wiring:

- `autorig-storage.service`: AutoRig backend. There is no `autorig.service` on
  this host; that name belongs to the retired VPS.
- Backend working directory: `/srv/autorig/current/autorig-online/backend`,
  which follows the release symlink.
- Backend command:

```bash
/srv/autorig/venv/bin/python3 -m uvicorn main:app --host 127.0.0.1 --port 8200 --workers 1 --no-access-log
```

- Backend listens on `127.0.0.1:8200`; Renderfin listens on `127.0.0.1:8210`.
- `autorig-storage-telegram.service`: AutoRig Telegram bot.
- nginx active site: `/etc/nginx/sites-enabled/autorig.online-storage`.
- nginx static root: `/srv/autorig/current/autorig-online/static`.
- nginx proxies API/backend traffic to `http://127.0.0.1:8200`.
- Converter nodes are reached through local SSH tunnels on `127.0.0.1:15xxx`,
  listed with a per-node token in `/srv/autorig/secrets/renderfin-hunyuan.json`.

For AutoRig changes, touch only `autorig-online/...` unless the user asks for
cross-service work.

### YouTube channel upload API

- The owner channel is `U3d Indie Game Developer` (`@unlim3d`), channel ID
  `UCpCN8wm6UXr8Ke_m-zSaThQ`. Google OAuth must be completed while signed in
  as `cgteamorg@gmail.com`; the OAuth callback must verify the authorized
  channel ID before saving the refresh token.
- OAuth uses only the `youtube.upload` scope and the production callback
  `https://autorig.online/api/oauth/youtube/callback`. Never put client secrets
  or refresh tokens in Git, browser storage, task output, or logs. Refresh
  tokens are stored in the server database `youtube_credentials` row. Use
  `YOUTUBE_GOOGLE_CLIENT_ID` and `YOUTUBE_GOOGLE_CLIENT_SECRET` for this
  integration; keep AutoRig sign-in's `GOOGLE_CLIENT_ID` and
  `GOOGLE_CLIENT_SECRET` unchanged.
- `/dev` keeps its Telegram Developer Validator UI and adds the proxied
  `POST /dev/api/youtube/videos` route. It accepts a video file plus
  title, description, optional comma-separated tags, and `privacy_status`.
  Require an admin session or an API key owned by an admin; never make this
  endpoint available to anonymous or ordinary user keys. Default visibility is
  `public`, matching the owner's request. YouTube may force private visibility
  until the API project passes its compliance audit; never claim public status
  without checking the uploaded video's actual state.
- YouTube classifies Shorts from the uploaded video's aspect ratio and length;
  the API has no Shorts flag. Check current YouTube rules before giving
  duration/format guidance. Long-form videos use the same resumable upload API.
- Stream multipart uploads from FastAPI's request-scoped `UploadFile` spool
  directly into the resumable YouTube upload, then close it. Do not make a
  second video copy or put uploads or OAuth credentials in the release tree.
- New or unverified YouTube Data API projects can be restricted to private
  uploads until YouTube completes its compliance audit. Never promise public
  publishing until a production upload confirms the project's current status.

### AutoRig Runtime Storage

AutoRig intentionally keeps generated task assets on disk so the public site
stays populated:

- `/srv/autorig/data/static/tasks`: cached public task downloads.
- `/srv/autorig/data/static/glb_cache`: cached model files for fast viewing.

Generated assets live under `/srv/autorig/data`, outside the release tree, and
nginx aliases them in; that is why a deploy can replace `current` without
losing them.
- `/var/autorig/videos`: cached task preview videos.
- `/var/autorig/uploads`: original uploaded source files.
- `/var/autorig/preflight-renders`: preflight poster/render files.

Do not delete these by age. Cleanup must be pressure-based: only run when root
free space is below the configured critical threshold. Prefer removing
regenerable ZIP bundles and old terminal-task upload originals before deleting
public task cache, GLB cache, videos, posters, or database task rows.

### AutoRig Deploy

Static-only changes:

```bash
ssh autorig-vps
CUR=$(readlink -f /srv/autorig/current)
sudo cp -a "$CUR" "$CUR-<change>"
sudo cp /tmp/<file> "$CUR-<change>/autorig-online/static/<file>"
sudo chown autorig:autorig "$CUR-<change>/autorig-online/static/<file>"
sudo ln -sfn "$CUR-<change>" /srv/autorig/current.new
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
curl -fsS https://autorig.online/gallery >/dev/null
```

A release is staged beside the live one and `current` is repointed at it, so
nothing is ever edited in place and a rollback is repointing the symlink back.
The deployed tree can be ahead of a local branch: carry only the changed files
rather than shipping a branch wholesale. Mirror whatever was deployed back into
`R:\autorig`, commit it, and push it.

Backend Python or dependency changes:

```bash
ssh autorig-vps
# stage a release as above, copy the backend files in, then:
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
sudo systemctl restart autorig-storage.service
systemctl status --no-pager autorig-storage.service
```

nginx config changes:

```bash
nginx -t
systemctl reload nginx
```

Useful health checks:

```bash
systemctl is-active autorig-storage.service nginx.service
curl -fsS 'http://127.0.0.1:8200/api/gallery?per_page=1&sort=date' >/dev/null
curl -fsS https://autorig.online/gallery >/dev/null
```

## worker-4090 Is This Computer

The render worker `worker-4090` (RTX 4090) is the owner's local workstation,
the same machine that holds `R:\autorig`. It is not a farm box and has no farm
SSH port: operate it locally.

* Runtime: ComfyUI 0.37.0 in `R:\autorig\.runtime\onlyrender`, listening on
  `127.0.0.1:8988`, exposed to the VPS by a reverse tunnel
  `ssh -R 19409:127.0.0.1:8988 autorig-vps` (renderfin sees it as
  `http://127.0.0.1:19409`).
* Controller: `autorig-online/deploy/onlyrender/worker-4090.ps1 -Mode
  Start|Stop|Status` (delegates to `worker-4090-v037.ps1` while
  `WAN2_PROMOTED` exists; `-Legacy` operates the old 0.21.1 runtime).
* Owner's desktop shortcuts: `worker-4090 START.bat` and
  `worker-4090 STOP.bat` on `C:\Users\user\Desktop`, calling the same
  controller.
* The GPU belongs to the owner first. After a reboot the worker is offline
  until started; `Stop` marks it offline, drains accepted renders, then frees
  the GPU. Do not start it without the owner's go-ahead.

## Other Custom VPS Services

The same VPS also hosts these custom services:

- `qwerty-gateway.service`: QwertyStock VPN Gateway, active, code at
  `/root/qwerty_vpn/gateway`, listens on `127.0.0.1:5000`.
- `qwerty-3proxy.service`: QwertyStock VPN 3proxy, active, config at
  `/root/qwerty_vpn/proxy/3proxy.cfg`, public proxy ports are in the
  `49152-49209` range.
- `renderfarmerbot.service`: RenderFarmer Telegram Bot, active, command
  `/usr/bin/python3 /root/renderfarmerbot.py`.
- `renderfarmer-watchdogg.service`: RenderFarmer watchdog, installed; it was
  observed in `activating auto-restart`, so inspect before relying on it.
- `cgtrader_submit.service`: CGTrader Submit Server, installed but observed
  inactive/dead, code at `/root/CGTrader_SUBMIT_SERVER`.
- `qwerty-autoreboot.service`: installed but observed inactive/dead.

Infrastructure services include nginx, ssh, cron, zabbix-agent, systemd
network/resolved/timesync, snap/cups, and QEMU guest agent.

Do not touch unrelated services when the request is about AutoRig only.

## Safety Rules

- Do not use production paths by guesswork; verify with `systemctl cat`,
  `readlink -f /etc/nginx/sites-enabled/...`, `ss -ltnp`, and `git status`.
- Do not run destructive git commands such as `git reset --hard` or
  `git checkout --` unless the user explicitly asks.
- If the repo is already dirty, preserve unrelated changes and stage only the
  files required for the task.
- Do not bypass login, captcha, 2FA, account lock, rate limits, or
  anti-automation screens during live tests.
- For frontend changes, verify live production HTML/JS with `curl` and browser
  testing against `https://autorig.online` when the UI behavior matters. Do not
  create local preview servers for AutoRig UI QA.
- Task viewer theme/backdrop assets are 16:9, not 9:16. Keep source JPGs in
  `static/env/backdrops/source/`, generate viewer derivatives as 16:9 JPGs
  (`1280x720` as the current target), and generate tiny strip thumbnails as
  16:9 JPGs (`160x90`). Do not replace them with vertical derivatives.

### SEO-Critical Layout

Google, Bing, Yandex, and other search engines are the primary traffic source.
Public navigation, footer links, and other crawl-critical internal links must be
present in the initial HTML returned by the server. Do not make SEO-critical
header/footer links depend only on client-side JavaScript rendering. Shared
layout partials are the canonical source for public header/footer markup; JS may
only enhance that markup for auth state, credits, language, theme, and menu
behavior.
