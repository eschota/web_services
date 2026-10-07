# worker-4090 AI node (LLM for the farm): starts whatever piece is missing, stops everything when the GPU is the
# owner's. Idempotent; run every minute by the task "AutoRig 4090 AI Node" through ensure-hidden.vbs (no console
# window ever flashes: games run on this PC).
#
#   ai_node.py  127.0.0.1:5480   the backend's AI contract (qwen35-9b-uncensored, llama-server on 127.0.0.1:8092)
#   ssh -R 15410:127.0.0.1:5480 autorig-vps   backend registry entry "worker-4090-ai" -> this node
#
# Owner 2026-10-07: «4090 и F12 … должны быть во флоте и рендерить ролики и обслуживать LLM запросы, если другие
# заняты». The card is the owner's first: the node runs only while the farm worker runs (ComfyUI listening on
# 127.0.0.1:8988, started by worker-4090.ps1 / the START shortcut). When the worker is stopped, or a file named
# DISABLED sits next to this script, the node and its tunnel are stopped. While it runs, ai_node.py itself yields
# the GPU to ComfyUI and to the 3D adapter and loads the model only when nvidia-smi shows room.
$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$logFile = Join-Path $here 'ensure.log'
$pidDir = Join-Path $here 'pids'
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null
$pyw = 'C:\Users\user\AppData\Local\Programs\Python\Python311\pythonw.exe'
$tunnelSpec = '15410:127.0.0.1:5480'

function Write-Log([string]$Message) {
    try {
        if ((Test-Path $logFile) -and (Get-Item $logFile).Length -gt 2MB) { Move-Item $logFile "$logFile.1" -Force }
        ('[{0}] {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Message) | Add-Content -Path $logFile -Encoding UTF8
    } catch { }
}
function Test-Url([string]$Url) {
    try { return (Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 8).StatusCode -eq 200 } catch { return $false }
}
function Get-PortOwner([int]$Port) {
    $hit = netstat -ano -p TCP | Select-String -Pattern ('^\s*TCP\s+127\.0\.0\.1:{0}\s+\S+\s+LISTENING\s+(\d+)' -f $Port) |
           Select-Object -First 1
    if ($hit) { return [int]$hit.Matches[0].Groups[1].Value }
    return $null
}
function Get-RecordedPid([string]$Name, [string]$ProcessName) {
    $file = Join-Path $pidDir "$Name.pid"
    if (-not (Test-Path $file)) { return 0 }
    $id = 0
    [void][int]::TryParse(((Get-Content $file -Raw) -as [string]).Trim(), [ref]$id)
    if ($id -le 0) { return 0 }
    $p = Get-Process -Id $id -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -eq $ProcessName) { return $id }
    return 0
}
function Set-RecordedPid([string]$Name, [int]$Id) { Set-Content -Path (Join-Path $pidDir "$Name.pid") -Value $Id }
function Stop-Recorded([string]$Name, [string]$ProcessName, [string]$Why) {
    $id = Get-RecordedPid $Name $ProcessName
    if ($id -gt 0) {
        Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
        Write-Log "$Name (pid $id) stopped: $Why"
    }
    Remove-Item (Join-Path $pidDir "$Name.pid") -ErrorAction SilentlyContinue
}

$why = ''
if (Test-Path (Join-Path $here 'DISABLED')) { $why = 'DISABLED file' }
elseif (-not (Get-PortOwner 8988)) { $why = 'the farm worker (ComfyUI 8988) is stopped: the GPU is the owner''s' }
if ($why) {
    # ai_node.py runs llama-server in a kill-on-close job object: stopping the node also stops the model.
    Stop-Recorded 'tunnel' 'ssh' $why
    Stop-Recorded 'ai_node' 'pythonw' $why
    exit 0
}

# the node
if (-not (Test-Url 'http://127.0.0.1:5480/healthz')) {
    Start-Sleep -Seconds 5
    if (-not (Test-Url 'http://127.0.0.1:5480/healthz')) {
        $known = Get-RecordedPid 'ai_node' 'pythonw'
        if ($known -gt 0) {
            Write-Log "ai_node (pid $known) does not answer /healthz; restarting it"
            Stop-Process -Id $known -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 3
        }
        $proc = Start-Process $pyw -ArgumentList @((Join-Path $here 'ai_node.py'), '--config', (Join-Path $here 'config.json')) `
            -WorkingDirectory $here -PassThru
        Set-RecordedPid 'ai_node' $proc.Id
        Write-Log "ai_node started (pid $($proc.Id))"
    }
}

# the tunnel
if ((Get-RecordedPid 'tunnel' 'ssh') -le 0) {
    $cleared = & ssh.exe -o BatchMode=yes -o ConnectTimeout=15 autorig-vps "bash ~/fleet4090/clear_stale_port.sh 15410" 2>&1
    if ($cleared) { Write-Log "VPS port 15410: $cleared" }
    $proc = Start-Process ssh.exe -WindowStyle Hidden -PassThru -ArgumentList @('-N', '-R', $tunnelSpec,
        '-o', 'ExitOnForwardFailure=yes', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=2',
        '-o', 'ConnectTimeout=20', '-o', 'BatchMode=yes', 'autorig-vps')
    Set-RecordedPid 'tunnel' $proc.Id
    Start-Sleep -Seconds 10
    if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) { Write-Log "tunnel $tunnelSpec started (pid $($proc.Id))" }
    else { Write-Log "tunnel $tunnelSpec exited right after start (network down or port still bound); next run retries" }
}
