# The farm

GPU boxes behind one gateway. Renderfin never talks to them directly over their
public HTTPS facades for anything authenticated — those strip the
`Authorization` header — so bearer-authenticated calls go through SSH tunnels to
each box's own loopback port.

## Boxes

| Box | Role | Tunnel (VPS → box) | SSH alias |
|---|---|---|---|
| f5 | ComfyUI render worker | `127.0.0.1:18488` → `:8488` | — |
| f15 | ComfyUI render worker | `127.0.0.1:18588` → `:8588` | — |
| f7 | Converter + Hunyuan3D 2.1 | `127.0.0.1:15131` → `:5131` | `farm-f7` |
| f13 | Converter + Hunyuan3D 2.1 | `127.0.0.1:15267` → `:5267` | `farm-f13` |
| Raptor | ComfyUI render worker | direct | — |

Gateway `5.129.157.224`. SSH to a converter box goes through it on a dedicated
port: f7 on 45131, f13 on 45267, key `~/.ssh/glb_converter_farm`.

The converter boxes run **Windows**. Commands go through PowerShell, and quoting
through `ssh` is fragile — write the script to a file and run it with
`-ExecutionPolicy Bypass -File "C:\Users\user\AppData\Local\Temp\x.ps1"` rather
than fighting nested quotes. An absolute path is required; `$env:TEMP` in the
`-File` argument is not expanded by the remote shell.

Converter code lives at `C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\`.
Hunyuan runtime at `C:\AI\HY3D2\` (on f7: `D:\AI\HY3D2\`).

## Tunnels

Unit `autorig-farm-tunnels`, config `/etc/autorig-farm-tunnels.conf`, one line
per tunnel: `<name> <ssh_port> <local_port> <remote_port>`. The supervisor exits
when any tunnel dies so systemd restarts the whole set.

The health check probes each local port and reports a tunnel that answers
nothing as down. An HTTP error response counts as **alive** — a status code
means the tunnel carried the request, which is all this check asks.

## Tokens

Each box provisions its own bearer token and **re-provisions it on restart**, so
a stale token is a routine condition rather than an incident. Renderfin's copy
lives in `/etc/autorig-renderfin-hunyuan.json` (mode 600).

On the box the live token is a file:
`%LOCALAPPDATA%\AutoRig\hunyuan_api_token`, read by `start_server_glb.bat` into
`HUNYUAN_API_TOKEN`. The converter validates it with `hmac.compare_digest` in
`validate_bearer_header` (`hunyuan3d_adapter.py`).

To refresh, see `runbook.md` — never print the token.

## Parking a worker

A box that is crashing or otherwise unusable is taken out of the pool with a
flag, not a deletion, so its url and token stay and restoring it is one word:

```json
{"name": "f7", "url": "http://127.0.0.1:15131", "token": "…",
 "enabled": false,
 "disabled_reason": "hard reboots (System event 41): 3 crashes on 2026-08-02"}
```

`config.hunyuan_workers()` skips entries with `enabled: false` or
`disabled: true` and prints the reason on every load, so nobody has to
rediscover why a box is missing. The health check reports a parked box as
parked rather than as a fault — a deliberate decision must not read the same as
a breakage.

Jobs already assigned to a parked box move on their own: `_stage_hunyuan_converter`
resolves the owning worker from the stored status url, finds it gone, and
resubmits. Restart renderfin after editing the file.

## Diagnosing a flaky box

Windows records a hard reboot as **System event 41** ("the system rebooted
without cleanly shutting down"). That single fact explained two unrelated-looking
symptoms on f7 at once: Blender killed mid-bake (leaving no Vertex-PBR manifest)
and `task vanished (HTTP 404)` from a box that came back with an empty task
registry.

```powershell
Get-WinEvent -FilterHashtable @{LogName='System'; Id=41,1074,6008; StartTime=(Get-Date).AddDays(-3)} |
  Select-Object -First 8 TimeCreated,Id
$os = Get-CimInstance Win32_OperatingSystem
[math]::Round(((Get-Date) - $os.LastBootUpTime).TotalHours,1)   # uptime
```

Event 1074 is a *clean* initiated restart and is not the same thing. Compare
uptime across boxes before blaming the pipeline: one crashing box in a pool of
two looks exactly like a broken pipeline.
