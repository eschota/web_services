# autorig-vehicle (Vehicles V3, owner orders 2026-10-15)

The vehicle conveyor's own service: `mt/vehicle_v3_service.py` from the MT truth tree
(`/srv/autorig/data/motion_transfer`), uvicorn on `127.0.0.1:8284`, nginx `location ^~ /api/mt/vehicle`.
Separate from autorig-mt so a vehicle release never needs an MT restart (the same pattern as autorig-gen3d).

Install (once):

    sudo install -m 644 autorig-vehicle.service /etc/systemd/system/autorig-vehicle.service
    # paste nginx-location.conf into /etc/nginx/sites-enabled/autorig.online-storage before `location ^~ /api/mt/`
    sudo nginx -t && sudo systemctl reload nginx
    sudo systemctl daemon-reload && sudo systemctl enable --now autorig-vehicle
    curl -s http://127.0.0.1:8284/api/mt/vehicle | head -c 300

A code change: `mt-deploy --mt-file mt/vehicle_v3.py=… --mt-file mt/vehicle_v3_service.py=…` (no `--restart`),
then `sudo systemctl restart autorig-vehicle` (only this unit; a run in flight resumes from its stage files).

## Fleet mask service (2026-10-15, owner: «нужно разнести это всё на флот, чтобы не один только 4090 участвовал в тестировании»)

`masks_service.py` (GroundingDINO-tiny + SAM2.1-tiny, `POST /masks`, `GET /health`) runs on every GPU box that can
take it, on `127.0.0.1:8989`, and the VPS sees each box at its own local URL. The client
`mt/vehicle_v3.masks_detect` takes the list (`VEHICLE_MASKS_URLS`, default = all of them; `VEHICLE_MASKS_URL` forces
one), keeps a 60 s health cache per URL, sends to the box with the fewest of its requests in flight (round robin among
equals), moves on to the next box on an error and uses the VPS CPU worker last. `masks.json` `source` is
`service:<box>:<gpu>`, plus `masks_box` / `masks_url`. `GET /api/fleet` shows `services_object.masks` per box and
`summary_object.masks_ready_array`.

| box | GPU | python (its own torch CUDA) | VPS URL | tunnel |
|---|---|---|---|---|
| worker-4090 | RTX 4090 | `R:\ComfyUI_windows_portable\python_embeded` | 127.0.0.1:19411 | reverse, `masks-4090.ps1` (not the task below) |
| raptor | RTX 3080 Ti | `D:\ComfyUI_windows_portable\python_embeded` (torch 2.9.1+cu130) | 127.0.0.1:19818 | `autorig-masks-raptor-tunnel.service`, `/srv/autorig/secrets/masks-raptor-tunnel.conf` |
| f12 | RTX 3080 Ti | `C:\AI\ComfyUI_windows_portable\python_embeded` (torch 2.9.1+cu130) | 127.0.0.1:19812 | reverse, f12 task «AutoRig F12 Masks Tunnel» (`C:\ProgramData\AutoRig\run_masks_tunnel_hidden.vbs` → `f12-reverse-tunnel.ps1 -RemotePort 19812 -LocalPort 8989 -Name masks`) |
| f5 | RTX 3070 Ti | `D:\ComfyUI_windows_portable\python_embeded` (torch 2.9.1+cu130) | 127.0.0.1:19805 | `autorig-masks-tunnels.service`, `/srv/autorig/secrets/masks-tunnels.conf` |
| f13, f1, f2 | GTX 1080 Ti | `C:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone` (torch 2.8.0+cu126; hydra/iopath/portalocker from `<root>\pylib`) | 19813, 19801, 19802 | `autorig-masks-tunnels.service` |
| f7 | GTX 1080 Ti | `D:\AI\HY3D2\...\python_standalone` | (19807, commented out) | installed, task **disabled**: the ai-node LLM holds the card, 149 s per picture |
| f15 | RTX 3070 Ti | — | — | not installed: no shell (SSH 48588 times out, LAN port 22 closed) |

On the box (`masks-box.ps1`, the same file in `C:\ProgramData\AutoRig\masks\`):

    masks-box.ps1 -Mode Install -Box f13 -Python <python.exe> -Payload masks-payload.zip [-IdleUnload 600]
    masks-box.ps1 -Mode Status|Start|Stop|Uninstall

* root `C:\ProgramData\AutoRig\masks\`: `grounding-dino\` (HF IDEA-Research/grounding-dino-tiny), `checkpoints\sam2.1_hiera_tiny.pt`,
  `sam2\` (the package from worker-4090 `.runtime\vehicle-seg\sam2`), `pylib\` (hydra, iopath, portalocker, appended to
  sys.path), `masks_service.py`, `run-masks.cmd` (loop, `PYTORCH_JIT=0`, offline HF), `logs\masks.out.log` (5 MB rotation).
* task «AutoRig Masks Service»: SYSTEM, at startup, restart on failure 999 × 1 min, no time limit.
* `--idle-unload`: 600 s on render boxes, 120 s on the 1080 Ti converters (Hunyuan needs the card); the next `/masks`
  loads again (+2–3 s). worker-4090 keeps them loaded.
* Payload: built once on worker-4090 (`masks-payload.zip`, 848 MB, sha256 `7886eedc…5def64`); home site → f12 over
  the LAN; farm site → Raptor once, then each farm box pulled it from Raptor's LAN server
  (`http://192.168.0.108:18998/_lan_masks/`, temporary, removed) with a sha256 check.
* Port forwarding: the farm boxes' `renderfin-farm-tunnel@autorig-vps` key line in
  `C:\ProgramData\ssh\administrators_authorized_keys` got `permitopen="127.0.0.1:8989"` (f1 f2 f5 f7 f13; backup
  `C:\ProgramData\AutoRig\masks-stage\administrators_authorized_keys.bak-20261015`). Raptor's key has no restriction.
* VPS units (`autorig-masks-tunnels.service`, `autorig-masks-raptor-tunnel.service`) run the same
  `deploy/renderfin-farm-tunnels/farm-tunnels.sh` as the converter tunnels with their own confs
  (`masks-tunnels.conf.example`), so adding a mask port never restarts the converter tunnels.
* Health from the VPS: `curl -s http://127.0.0.1:19818/health` (ok, loaded, gpu, requests, inflight, last_seconds, vram_mb).
