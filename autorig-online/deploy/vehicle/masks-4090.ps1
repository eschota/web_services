# The GPU mask service on worker-4090 (owner 2026-10-15: «сразу делай чтобы мы могли сегментировать быстро»).
#   masks-4090.ps1 -Mode Start|Stop|Status
# Runs deploy/vehicle/masks_service.py under the ComfyUI python (torch CUDA) on 127.0.0.1:8989 and a reverse SSH
# tunnel so the VPS sees it as http://127.0.0.1:19410 (mt/vehicle_v3.masks_detect, VEHICLE_MASKS_URL).
param([ValidateSet("Start", "Stop", "Status")][string]$Mode = "Status")
$ErrorActionPreference = "Stop"
$Py = "R:\ComfyUI_windows_portable\python_embeded\python.exe"
$Root = "R:\autorig\.runtime\vehicle-seg"
$Script = "R:\autorig\autorig-online\deploy\vehicle\masks_service.py"
$Port = 8989
$Tunnel = 19410
$LogDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force $LogDir | Out-Null
$PidFile = Join-Path $Root "masks.pid"
$TunPidFile = Join-Path $Root "tunnel.pid"

function Get-Proc($file) {
    if (Test-Path $file) { $id = Get-Content $file | Select-Object -First 1; if ($id) { return Get-Process -Id ([int]$id) -ErrorAction SilentlyContinue } }
    return $null
}

switch ($Mode) {
    "Start" {
        if (-not (Get-Proc $PidFile)) {
            $p = Start-Process -FilePath $Py -ArgumentList @($Script, "--root", $Root, "--port", "$Port", "--device", "cuda") `
                -WorkingDirectory $Root -RedirectStandardOutput (Join-Path $LogDir "masks.out.log") `
                -RedirectStandardError (Join-Path $LogDir "masks.err.log") -WindowStyle Hidden -PassThru
            $p.Id | Set-Content $PidFile
            Write-Output "masks service started pid $($p.Id)"
        } else { Write-Output "masks service already running" }
        if (-not (Get-Proc $TunPidFile)) {
            $t = Start-Process -FilePath "ssh" -ArgumentList @("-N", "-o", "ServerAliveInterval=30", "-o", "ExitOnForwardFailure=yes", "-R", "${Tunnel}:127.0.0.1:${Port}", "autorig-vps") `
                -WindowStyle Hidden -PassThru
            $t.Id | Set-Content $TunPidFile
            Write-Output "tunnel started pid $($t.Id) (VPS 127.0.0.1:$Tunnel -> local $Port)"
        } else { Write-Output "tunnel already running" }
    }
    "Stop" {
        foreach ($f in @($TunPidFile, $PidFile)) { $pr = Get-Proc $f; if ($pr) { Stop-Process -Id $pr.Id -Force; Write-Output "stopped $($pr.Id)" }; Remove-Item $f -ErrorAction SilentlyContinue }
    }
    "Status" {
        $pr = Get-Proc $PidFile; $t = Get-Proc $TunPidFile
        Write-Output ("service: " + $(if ($pr) { "running pid $($pr.Id)" } else { "stopped" }))
        Write-Output ("tunnel:  " + $(if ($t) { "running pid $($t.Id)" } else { "stopped" }))
        try { Write-Output ("health:  " + (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 3).Content) } catch { Write-Output "health:  no answer" }
    }
}
