# Keeps the local RTX 4090 Hunyuan3D image-to-3D service reachable from the
# autorig VPS for the renderfin character-generation pipeline.
#
# 1. Starts the Hunyuan-only Flask service on 127.0.0.1:17013 (if not running).
# 2. Maintains a reverse SSH tunnel so the VPS reaches it at 127.0.0.1:17013.
#
# The VPS renderfin service must have, in /etc/autorig-renderfin.env:
#   HUNYUAN_API_TOKEN=<contents of C:\AI\HY3D2\secrets\hunyuan_api_token>
#   RENDERFIN_HUNYUAN_WORKERS=http://127.0.0.1:17013
#
# Run at logon via Task Scheduler (Run whether user is logged on or not).

$ErrorActionPreference = 'Stop'

$PythonExe = 'R:\SECS\.ai-tools\hunyuan3d\runtime\Hunyuan3D2_WinPortable\python_standalone\python.exe'
$Launcher  = 'C:\AI\HY3D2\server\launch_hunyuan_localhost.py'
$RepoRoot  = 'R:\3d_hunyuan_rollout_commit'
$LocalPort = 17013
$VpsHost   = 'autorig-vps'   # from ~/.ssh/config

function Test-LocalHunyuan {
    try {
        $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 `
            "http://127.0.0.1:$LocalPort/api-converter-glb/server-status"
        return $r.StatusCode -eq 200
    } catch { return $false }
}

if (-not (Test-LocalHunyuan)) {
    Write-Host "Starting local Hunyuan service on :$LocalPort ..."
    Start-Process -FilePath $PythonExe -ArgumentList @($Launcher) `
        -WorkingDirectory $RepoRoot -WindowStyle Hidden
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 2
        if (Test-LocalHunyuan) { break }
    }
    if (-not (Test-LocalHunyuan)) { throw "Hunyuan service did not come up on :$LocalPort" }
}
Write-Host "Local Hunyuan service is up."

# Maintain the reverse tunnel in the foreground (Task Scheduler restarts on exit).
Write-Host "Opening reverse tunnel to $VpsHost ..."
& ssh -N `
    -o ExitOnForwardFailure=yes `
    -o ServerAliveInterval=30 `
    -o ServerAliveCountMax=3 `
    -R "127.0.0.1:${LocalPort}:127.0.0.1:${LocalPort}" `
    $VpsHost
