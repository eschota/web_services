# f12 AI node (LLM for the farm): starts the node when it is missing, stops it when the farm ComfyUI is not running.
# Idempotent; run every minute by the task "AutoRig F12 AI Node" through ensure-hidden.vbs. The tunnel
# WAY 127.0.0.1:15412 -> 127.0.0.1:5480 is the task "AutoRig F12 AI Tunnel" (f12-reverse-tunnel.ps1).
#
# Owner 2026-10-07: «4090 и F12 … должны быть во флоте и рендерить ролики и обслуживать LLM запросы, если другие
# заняты». The node runs only while the farm ComfyUI backend listens on 127.0.0.1:8289 (behind the GPU arbiter);
# a file named DISABLED next to this script stops it. While it runs, ai_node.py yields the GPU to ComfyUI.
$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$logFile = Join-Path $here 'ensure.log'
$pidDir = Join-Path $here 'pids'
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null
$pyw = 'C:\AI\ComfyUI_windows_portable\python_embeded\pythonw.exe'

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

$why = ''
if (Test-Path (Join-Path $here 'DISABLED')) { $why = 'DISABLED file' }
elseif (-not (Get-PortOwner 8289)) { $why = 'the farm ComfyUI backend (8289) is not running' }
if ($why) {
    $id = Get-RecordedPid 'ai_node' 'pythonw'
    if ($id -gt 0) {
        Stop-Process -Id $id -Force -ErrorAction SilentlyContinue      # its job object takes llama-server down too
        Write-Log "ai_node (pid $id) stopped: $why"
    }
    Remove-Item (Join-Path $pidDir 'ai_node.pid') -ErrorAction SilentlyContinue
    exit 0
}
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
        Set-Content -Path (Join-Path $pidDir 'ai_node.pid') -Value $proc.Id
        Write-Log "ai_node started (pid $($proc.Id))"
    }
}
