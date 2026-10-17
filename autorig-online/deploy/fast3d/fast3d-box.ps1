# AutoRig Fast 3D (Hunyuan3D-2mini) on a GPU box (owner 2026-10-17: «делай мини-30»; «чтобы 3д модель появлялась
# через 5 секунд»; AGENTS.md «Fast 3D Generator On Every Box»). Service: deploy/fast3d/fast3d_service.py.
#
#   fast3d-box.ps1 -Mode Install -Box raptor [-Python <torch-CUDA python.exe>] [-Hunyuan <Hunyuan3D-2 dir>] [-Pylib DIR]
#                  [-Dtype fp16|fp32] [-MaxBatch N] [-MinFreeMb MB] [-User SYSTEM|<user>]
#   fast3d-box.ps1 -Mode Start|Stop|Status|Uninstall
#
# Weights: $Root\models\tencent\Hunyuan3D-2mini\{hunyuan3d-dit-v2-mini,hunyuan3d-dit-v2-mini-turbo}\{config.yaml,
# model.fp16.safetensors} (3.8 GB each, DiT + VAE + DINOv2 in one file), from the internet once per site, then over the
# site's LAN (sha256 3cc66f3b... mini, bdbcef30... mini-turbo), plus hunyuan3d-vae-v2-mini-turbo (407 MB, the FlashVDM VAE).
# Install writes run-fast3d.cmd beside fast3d_service.py (already copied into -Root) and registers the scheduled task
# «AutoRig Fast 3D» (SYSTEM, at startup, restart on failure) on 127.0.0.1:$Port. The VPS reaches it through the box's
# SSH tunnel (raptor: autorig-masks-raptor-tunnel.service, conf line raptor-fast3d 48288 19828 8990; the VPS-side URLs
# are gen3d-routing.json farm.mini_boxes and the fleet API's FAST3D_URLS).
param(
    [ValidateSet("Install", "Start", "Stop", "Status", "Uninstall")][string]$Mode = "Status",
    [string]$Root = "C:\ProgramData\AutoRig\fast3d",
    [string]$Python = "D:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone\python.exe",
    [string]$Hunyuan = "D:\AI\HY3D2\Hunyuan3D2_WinPortable\Hunyuan3D-2",
    [string]$Hf = "X:\orbit\nvs\hf",
    [string]$Pylib = "",
    [ValidateSet("fp16", "fp32")][string]$Dtype = "fp16",
    [int]$MaxBatch = 3,
    [string]$Box = $env:COMPUTERNAME.ToLower(),
    [int]$Port = 8990,
    [int]$IdleUnload = 900,
    [int]$MinFreeMb = 5200,
    [string]$User = "SYSTEM"
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$TaskName = "AutoRig Fast 3D"
function Health { try { return (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 5).Content } catch { return "" } }
function Kill-Service {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -like "*fast3d_service.py*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "killed python $($_.ProcessId)" }
}
switch ($Mode) {
    "Install" {
        foreach ($need in @($Python, (Join-Path $Hunyuan "hy3dgen\shapegen\pipelines.py"), (Join-Path $Root "fast3d_service.py"))) {
            if (-not (Test-Path $need)) { throw "missing $need" }
        }
        New-Item -ItemType Directory -Force (Join-Path $Root "logs") | Out-Null
        $log = Join-Path $Root "logs\fast3d.out.log"
        $svc = Join-Path $Root "fast3d_service.py"
        $cmd = @"
@echo off
rem AutoRig Fast 3D on $Box (deploy/fast3d/fast3d-box.ps1). Loops forever; the task restarts it if cmd dies.
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
set HY3DGEN_MODELS=$Root\models
cd /d "$Root"
:loop
for %%F in ("$log") do if %%~zF GTR 5000000 move /y "$log" "$log.1" >nul
"$Python" -X utf8 "$svc" --root "$Root" --port $Port --box $Box --hunyuan "$Hunyuan" --hf "$Hf" --pylib "$Pylib" --dtype $Dtype --max-batch $MaxBatch --idle-unload $IdleUnload --min-free-mb $MinFreeMb >> "$log" 2>&1
ping -n 11 127.0.0.1 >nul
goto loop
"@
        Set-Content -Path (Join-Path $Root "run-fast3d.cmd") -Value $cmd -Encoding ASCII
        $a = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$Root\run-fast3d.cmd`"" -WorkingDirectory $Root
        $s = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
            -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew
        if ($User -eq "SYSTEM") {
            $t = New-ScheduledTaskTrigger -AtStartup
            $p = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
        } else {                                          # worker-4090: the owner's own session (his GPU first)
            $t = New-ScheduledTaskTrigger -AtLogOn -User $User
            $p = New-ScheduledTaskPrincipal -UserId $User -LogonType Interactive -RunLevel Limited
        }
        Register-ScheduledTask -TaskName $TaskName -Action $a -Trigger $t -Settings $s -Principal $p `
            -Description "AutoRig Fast 3D: Hunyuan3D-2mini (mini30 / mini-turbo) on 127.0.0.1:$Port; deploy/fast3d/fast3d-box.ps1" -Force | Out-Null
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Kill-Service
        Start-ScheduledTask -TaskName $TaskName
        Start-Sleep 12
        "health: " + (Health)
    }
    "Start" { Start-ScheduledTask -TaskName $TaskName; Start-Sleep 10; "health: " + (Health) }
    "Stop" { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue; Kill-Service }
    "Uninstall" { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue; Kill-Service; Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }
    default { (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Select-Object TaskName, State | Out-String); "health: " + (Health) }
}
