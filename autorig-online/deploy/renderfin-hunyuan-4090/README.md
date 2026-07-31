# Renderfin ↔ local RTX 4090 Hunyuan3D

The renderfin character-generation pipeline (Telegram "🎨 Сгенерировать" button)
turns the isolated Flux T-pose render into a 3D GLB via Hunyuan3D 2.1.

The farm converter workers (F2/F7/F13) expose that API, but each holds its own
`HUNYUAN_API_TOKEN` that is provisioned on-box and not stored in the repo. When
those tokens are not available to the VPS, the pipeline uses the **local RTX 4090**
Hunyuan runtime on `DESKTOP-QTG6T29` instead, bridged to the VPS over a reverse
SSH tunnel.

## Components

- **Local service**: `C:\AI\HY3D2\server\launch_hunyuan_localhost.py` runs the
  Hunyuan-only Flask API on `127.0.0.1:17013`. Token: `C:\AI\HY3D2\secrets\hunyuan_api_token`.
- **Tunnel**: `ssh -R 127.0.0.1:17013:127.0.0.1:17013 autorig-vps` publishes it on
  the VPS loopback.
- **VPS renderfin** (`/etc/autorig-renderfin.env`):
  ```
  HUNYUAN_API_TOKEN=<contents of C:\AI\HY3D2\secrets\hunyuan_api_token>
  RENDERFIN_HUNYUAN_WORKERS=http://127.0.0.1:17013
  ```

## Keep it running

`start_local_hunyuan_and_tunnel.ps1` starts the service (if down) and holds the
tunnel open. Register it in Task Scheduler ("Run whether user is logged on or
not", restart on failure) so the 3D stage survives reboots.

## Switching to the farm workers

Once F2/F7/F13 tokens are available, drop the tunnel and set on the VPS:
```
RENDERFIN_HUNYUAN_WORKERS=https://converter-f2.freestock.online,https://converter-f7.freestock.online,https://converter-f13.freestock.online
HUNYUAN_API_TOKEN=<shared/valid worker token>
```
then `systemctl restart autorig-renderfin`. The client auto-picks an idle worker.

Note: `generate-3d` requires the input `image_url` to be a **public** http(s) URL
on port 80/443 — renderfin passes the isolated render at
`https://autorig.online/renderfin/render/<user>/<id>_Isolated.png`, which qualifies.
