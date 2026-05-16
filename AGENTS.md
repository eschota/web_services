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

Production VPS SSH access is configured in the user's `~/.ssh/config` as:

```bash
ssh autorig-vps
```

The production git root on the VPS is:

```text
/root
```

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
7. On the VPS, deploy by pulling in `/root` only when it is safe:

```bash
ssh autorig-vps
cd /root
git pull
```

If `/root` has server-side drift from GitHub, do not run a blind `git pull`.
Deploy the exact changed files with a targeted patch/copy, or first resolve the
repository divergence as a separate task.

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

Production application path:

```text
/root/autorig-online
```

Do not use `/opt/autorig-online` as production. Do not deploy from `/opt`.
There may also be a small `/autorig-online` directory at filesystem root; it is
not the current backend checkout.

Active production wiring:

- `autorig.service`: AutoRig backend.
- Backend working directory: `/root/autorig-online/backend`.
- Backend command:

```bash
/root/autorig-online/venv/bin/python3 -m uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
```

- Backend listens on `127.0.0.1:8000`.
- `autorig-telegram.service`: AutoRig Telegram bot.
- nginx active site: `/etc/nginx/sites-enabled/autorig.online`.
- nginx static root: `/root/autorig-online/static`.
- nginx proxies API/backend traffic to `http://127.0.0.1:8000`.

For AutoRig changes, touch only `autorig-online/...` unless the user asks for
cross-service work.

### AutoRig Runtime Storage

AutoRig intentionally keeps generated task assets on disk so the public site
stays populated:

- `/root/autorig-online/static/tasks`: cached public task downloads.
- `/root/autorig-online/static/glb_cache`: cached model files for fast viewing.
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
cd /root
git pull
curl -fsS https://autorig.online/gallery >/dev/null
```

Use the `git pull` deploy path only when `/root` is clean and aligned with the
remote repository. If it is not aligned, apply a targeted patch/copy to
`/root/autorig-online` and then copy the same change back into `R:\autorig`,
commit it, and push it.

Backend Python or dependency changes:

```bash
ssh autorig-vps
cd /root
git pull
systemctl restart autorig.service
systemctl status --no-pager autorig.service
```

nginx config changes:

```bash
nginx -t
systemctl reload nginx
```

Useful health checks:

```bash
systemctl is-active autorig.service nginx.service
curl -fsS 'http://127.0.0.1:8000/api/gallery?per_page=1&sort=date' >/dev/null
curl -fsS https://autorig.online/gallery >/dev/null
```

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

### SEO-Critical Layout

Google, Bing, Yandex, and other search engines are the primary traffic source.
Public navigation, footer links, and other crawl-critical internal links must be
present in the initial HTML returned by the server. Do not make SEO-critical
header/footer links depend only on client-side JavaScript rendering. Shared
layout partials are the canonical source for public header/footer markup; JS may
only enhance that markup for auth state, credits, language, theme, and menu
behavior.
