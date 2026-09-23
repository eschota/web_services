# Supervisor for the isolated ComfyUI image worker on a GTX 1080 Ti converter
# box: ComfyUI on 127.0.0.1:8389 behind comfy1080_guard.py on 127.0.0.1:8388.
# Runs from the scheduled task "AutoRig Comfy1080" (at startup, as the same
# account as the converter). Restarts either process if it dies. A file
# C:\ProgramData\AutoRig\comfy1080\DISABLED stops both and keeps them stopped.
param(
    [string]$Root = 'C:\AI\ComfyUI1080',
    [int]$ConverterPort = 0,
    [int]$Settle = 60,
    [double]$MinFreeRamGb = 10,
    # Pascal-safe set, measured on f11 (GTX 1080 Ti, torch 2.8.0+cu126) on
    # 2026-09-23. PyTorch SDPA ("pytorch attention") and the Hunyuan runtime's
    # xformers both abort inside attention on sm_61 and take the driver down
    # (nvlddmkm Xid 13 + TDR, LiveKernelEvent 141); quad/split attention do
    # not. fp32 compute keeps the text encoder numerics sane and is as fast as
    # fp16 here (Pascal has no fast fp16). DynamicVRAM stays on: it is what
    # lets Z-Image load fully instead of streaming half the model.
    [string]$ComfyFlags = '--disable-cuda-graphs --disable-comfy-compiler --disable-xformers --disable-async-offload --use-quad-cross-attention --force-fp32'
)
$ErrorActionPreference = 'Continue'
$state = 'C:\ProgramData\AutoRig\comfy1080'
New-Item -ItemType Directory -Force -Path $state | Out-Null
$log = Join-Path $state 'run.log'
function Log($m) { Add-Content -Path $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) }
if ($ConverterPort -le 0) {
    $map = @{ 'F1-PC' = 5132; 'F2-PC' = 5279; 'F7-PC' = 5131; 'F11-PC' = 5533; 'F13-PC' = 5267 }
    $ConverterPort = $map[$env:COMPUTERNAME.ToUpper()]
}
if (-not $ConverterPort) { Log "no converter port for $env:COMPUTERNAME"; exit 2 }
$py = Join-Path $Root 'python\python.exe'
$comfy = Join-Path $Root 'ComfyUI'
$guard = Join-Path $state 'comfy1080_guard.py'
$env:PYTHONNOUSERSITE = '1'
$env:PYTHONUNBUFFERED = '1'

function Clear-Port([int]$port) {
    # An orphan from an earlier supervisor (killed task, crash) would keep the
    # port and make every restart fail to bind. Only our own python is killed.
    foreach ($c in @(Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue)) {
        $p = Get-CimInstance Win32_Process -Filter "ProcessId=$($c.OwningProcess)" -ErrorAction SilentlyContinue
        if ($p -and $p.ExecutablePath -like 'C:\AI\ComfyUI1080\*') {
            Log "killing orphan pid=$($p.ProcessId) on port $port"
            Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 2
        }
    }
}
function Start-Comfy {
    Clear-Port 8389
    $argv = @('main.py', '--listen', '127.0.0.1', '--port', '8389', '--disable-auto-launch',
              '--disable-smart-memory', '--preview-method', 'none') + @($ComfyFlags.Split(' ') | Where-Object { $_ })
    Start-Process -FilePath $py -ArgumentList $argv -WorkingDirectory $comfy -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $state 'comfy.out.log') -RedirectStandardError (Join-Path $state 'comfy.err.log') -PassThru
}
function Start-Guard {
    Clear-Port 8388
    $argv = @($guard, '--converter-port', "$ConverterPort", '--settle', "$Settle",
              '--min-free-ram-gb', "$MinFreeRamGb", '--state-file', (Join-Path $state 'guard.json'))
    Start-Process -FilePath $py -ArgumentList $argv -WorkingDirectory $state -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $state 'guard.out.log') -RedirectStandardError (Join-Path $state 'guard.err.log') -PassThru
}
function Rotate($name) {
    # Keep the log of the run that just ended: a crash is diagnosed from it.
    $p = Join-Path $state $name
    if (Test-Path $p) { Move-Item -Force $p ($p + '.prev') }
}

Log "supervisor start converter=$ConverterPort"
$c = $null; $g = $null; $lastPrune = Get-Date '2000-01-01'
while ($true) {
    if (Test-Path (Join-Path $state 'DISABLED')) {
        foreach ($p in @($g, $c)) { if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force } }
        $c = $null; $g = $null
        Start-Sleep -Seconds 30; continue
    }
    if (-not $c -or $c.HasExited) {
        if ($c) { Log "comfy exited code=$($c.ExitCode)" }
        Rotate 'comfy.out.log'; Rotate 'comfy.err.log'
        $c = Start-Comfy; Log "comfy pid=$($c.Id)"
    }
    if (-not $g -or $g.HasExited) {
        if ($g) { Log "guard exited code=$($g.ExitCode)" }
        Rotate 'guard.out.log'; Rotate 'guard.err.log'
        $g = Start-Guard; Log "guard pid=$($g.Id)"
    }
    if (((Get-Date) - $lastPrune).TotalHours -ge 1) {
        # Renderfin downloads every artifact right after the prompt; keep two days.
        foreach ($d in 'output', 'input', 'temp') {
            Get-ChildItem (Join-Path $comfy $d) -Recurse -File -ErrorAction SilentlyContinue |
                Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-2) } |
                Remove-Item -Force -ErrorAction SilentlyContinue
        }
        $lastPrune = Get-Date
    }
    Start-Sleep -Seconds 10
}
