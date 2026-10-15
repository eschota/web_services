# The GPU mask service on worker-4090 (owner 2026-10-15: «сразу делай чтобы мы могли сегментировать быстро»).
#   masks-4090.ps1 -Mode Start|Stop|Status
# Runs deploy/vehicle/masks_service.py under the ComfyUI python (torch CUDA) on 127.0.0.1:8989 and a reverse SSH
# tunnel so the VPS sees it as http://127.0.0.1:19411 (mt/vehicle_v3.masks_detect, VEHICLE_MASKS_URL).
param([ValidateSet("Start", "Stop", "Status")][string]$Mode = "Status")
$ErrorActionPreference = "Stop"
$env:PYTORCH_JIT = "0"   # the embedded python has no stdlib source: TorchScript cannot script Enum, so scripting is off
$Py = "R:\ComfyUI_windows_portable\python_embeded\python.exe"
$Root = "R:\autorig\.runtime\vehicle-seg"
$Script = "R:\autorig\autorig-online\deploy\vehicle\masks_service.py"
$Port = 8989
$Tunnel = 19411   # 19410 is taken by another reverse tunnel on this PC (-> 8987)
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
            # a watchdog loop, not one ssh: the VPS drops ssh sessions several times an hour (2026-10-15 the tunnel died
            # and the split stage fell back to the CPU worker, 119 s instead of 2 s), so ssh is restarted 5 s after it exits
            $loop = "while (`$true) { ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -R ${Tunnel}:127.0.0.1:${Port} autorig-vps; Start-Sleep 5 }"
            $t = Start-Process -FilePath "powershell" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $loop) -WindowStyle Hidden -PassThru
            $t.Id | Set-Content $TunPidFile
            Write-Output "tunnel watchdog started pid $($t.Id) (VPS 127.0.0.1:$Tunnel -> local $Port)"
        } else { Write-Output "tunnel already running" }
    }
    "Stop" {
        $tw = Get-Proc $TunPidFile
        if ($tw) { Get-CimInstance Win32_Process -Filter "ParentProcessId = $($tw.Id)" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } }
        foreach ($f in @($TunPidFile, $PidFile)) { $pr = Get-Proc $f; if ($pr) { Stop-Process -Id $pr.Id -Force; Write-Output "stopped $($pr.Id)" }; Remove-Item $f -ErrorAction SilentlyContinue }
    }
    "Status" {
        $pr = Get-Proc $PidFile; $t = Get-Proc $TunPidFile
        Write-Output ("service: " + $(if ($pr) { "running pid $($pr.Id)" } else { "stopped" }))
        $sshUp = $false
        if ($t) { $sshUp = [bool](Get-CimInstance Win32_Process -Filter "ParentProcessId = $($t.Id) and Name = 'ssh.exe'") }
        Write-Output ("tunnel:  " + $(if ($t) { "watchdog pid $($t.Id), ssh " + $(if ($sshUp) { "up" } else { "reconnecting" }) } else { "stopped" }))
        try { Write-Output ("health:  " + (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 3).Content) } catch { Write-Output "health:  no answer" }
    }
}
