# RenderFarmer Monitor v3

`renderfarmerbot.py` is a low-noise Telegram dashboard for the production
AutoRig/Renderfin farm. It runs on the `way-fr` storage host, beside the live
databases and internal APIs, rather than on the retired VPS checkout.

## Notification contract

- One persistent dashboard message is reused per subscribed chat.
- The farm is polled every 60 seconds, but Telegram is edited only when the
  semantic state changes (queue counts, worker health, pool membership, task
  completion counters, or storage health band).
- Message IDs and delivered fingerprints are persisted atomically across
  restarts in `/srv/autorig/data/var/renderfarmer-monitor/state.json`.
- A missing Telegram message is replaced once. A blocked/deleted chat is
  unsubscribed instead of retried forever.
- Startup broadcasts, automatic task media groups, completion videos, and the
  second "completed tasks" message are intentionally disabled.
- HTTP client INFO logs are disabled because Telegram request URLs contain the
  bot token.

## Live sources

- AutoRig conversion queue: `http://127.0.0.1:8200/api/queue/status`
- Renderfin/Comfy/Hunyuan pool: `http://127.0.0.1:8210/renderfin/health`
- Converter `/server-status`: F1, F2, F11, and F13
- F7 is shown as parked/disabled and is not treated as an outage.
- Disk usage is measured on `/srv/autorig` on `way-fr`.

The Hunyuan dedicated/shared membership is read from Renderfin health at each
poll, so the dashboard follows production admission-control changes without a
separate hard-coded Hunyuan worker list.

## Service and secrets

Install `autorig-online/deploy/renderfarmer-monitor.service` as:

```bash
sudo install -m 0644 autorig-online/deploy/renderfarmer-monitor.service \
  /etc/systemd/system/renderfarmer-monitor.service
```

Create `/srv/autorig/secrets/renderfarmer-monitor.env` as a root-owned `0600`
file containing only:

```text
TELEGRAM_BOT_TOKEN=...
```

Do not put the token in `ExecStart`, command-line arguments, or a systemd
`Environment=` line. Do not print the EnvironmentFile or enable HTTP request
logging in production.

Legacy `renderfarmerbot.service` and `renderfarmer-watchdogg.service` on the old
VPS must stay disabled. The watchdog is retired because systemd already handles
process failures; its former whole-`/root` source watcher caused restart storms.

## Validation

Read-only source check (does not contact Telegram):

```bash
/srv/autorig/venv/bin/python3 /srv/autorig/current/renderfarmerbot.py --check
```

Service checks:

```bash
systemctl status --no-pager renderfarmer-monitor.service
journalctl -u renderfarmer-monitor.service --since '-10 min' --no-pager
```

After the first state-changing edit, leave the farm unchanged for at least two
poll intervals. The journal must contain no additional Telegram dashboard
updates and `state.json` must retain the same message IDs and fingerprints.
