# AutoRig Fast 3D on worker-4090 (this PC, the owner's own GPU; AGENTS.md «Fast 3D Generator On Every Box, worker-4090
# First And Batched», 2026-10-17). The service is deploy/fast3d/fast3d_service.py under the render worker's ComfyUI
# python (R:\ComfyUI_windows_portable\python_embeded) with its own overlay R:\autorig\.runtime\fast3d\pylib (trimesh,
# pymeshlab, pyfqmr; nothing is installed into the ComfyUI environment) and the hy3dgen shape code of Hunyuan3D-2 in
# R:\autorig\.runtime\fast3d\Hunyuan3D-2. Weights: R:\autorig\.runtime\fast3d\models\tencent\Hunyuan3D-2mini (the home
# site's one download, 2026-10-17). The VPS sees it as http://127.0.0.1:19832 through a reverse tunnel under a watchdog
# loop (as masks-4090.ps1).
#
#   fast3d-4090.ps1 -Mode Start|Stop|Status
param(
    [ValidateSet("Start", "Stop", "Status")][string]$Mode = "Status",
    [string]$Root = "R:\autorig\.runtime\fast3d",
    [string]$Python = "R:\ComfyUI_windows_portable\python_embeded\python.exe",
    [int]$Port = 8990,
    [int]$Tunnel = 19832,
    [int]$MaxBatch = 8,
    [int]$IdleUnload = 1800,
    [int]$MinFreeMb = 5200
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$pidFile = Join-Path $Root "fast3d.pid"
$tunFile = Join-Path $Root "tunnel.pid"
function Health { try { return (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 5).Content } catch { return "" } }
function Get-Svc { Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -like "*fast3d_service.py*" } }
function Get-Tun { if (Test-Path $tunFile) { Get-Process -Id ([int](Get-Content $tunFile)) -ErrorAction SilentlyContinue } }
switch ($Mode) {
    "Start" {
        New-Item -ItemType Directory -Force (Join-Path $Root "logs") | Out-Null
        if (-not (Get-Svc)) {
            # a watchdog loop (run-fast3d.cmd): the service is restarted 5 s after it exits (a native crash leaves no
            # python to answer)
            $cmd = @"
@echo off
set HY3DGEN_MODELS=$Root\models
set HF_HOME=$Root\hf
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
rem pymeshlab's floater pass writes temp PLYs: never on C: (2 GB free on this PC, 2026-10-17 «Output Stream Error»)
set TMP=$Root\tmp
set TEMP=$Root\tmp
cd /d "$Root"
:loop
"$Python" -X utf8 "$Root\fast3d_service.py" --root "$Root" --port $Port --box worker-4090 --hunyuan "$Root\Hunyuan3D-2" --pylib "$Root\pylib" --hf "$Root\hf" --max-batch $MaxBatch --idle-unload $IdleUnload --min-free-mb $MinFreeMb --comfy http://127.0.0.1:8988 --keep 2 --yield-3d 1 --exit-on-unload 1 >> "$Root\logs\fast3d.out.log" 2>> "$Root\logs\fast3d.err.log"
ping -n 6 127.0.0.1 >nul
goto loop
"@
            Set-Content -Path (Join-Path $Root "run-fast3d.cmd") -Value $cmd -Encoding ASCII
            $p = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", (Join-Path $Root "run-fast3d.cmd")) -WorkingDirectory $Root -WindowStyle Hidden -PassThru
            Set-Content $pidFile $p.Id
        }
        if (-not (Get-Tun)) {
            $loop = "while (`$true) { ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -R ${Tunnel}:127.0.0.1:${Port} autorig-vps; Start-Sleep 5 }"
            $t = Start-Process -FilePath "powershell" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $loop) -WindowStyle Hidden -PassThru
            Set-Content $tunFile $t.Id
        }
        Start-Sleep 10
        "health: " + (Health)
    }
    "Stop" {
        $t = Get-Tun
        if ($t) { Get-CimInstance Win32_Process -Filter "ParentProcessId = $($t.Id)" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }; Stop-Process -Id $t.Id -Force }
        if (Test-Path $pidFile) { Stop-Process -Id ([int](Get-Content $pidFile)) -Force -ErrorAction SilentlyContinue }   # the cmd loop first
        Get-Svc | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "stopped $($_.ProcessId)" }
        Remove-Item $pidFile, $tunFile -ErrorAction SilentlyContinue
    }
    default {
        $s = Get-Svc
        "service: " + $(if ($s) { "pid " + (($s | ForEach-Object ProcessId) -join ",") } else { "stopped" })
        $t = Get-Tun
        "tunnel:  " + $(if ($t) { "watchdog pid $($t.Id)" } else { "stopped" })
        "health: " + (Health)
    }
}
