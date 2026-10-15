# The vehicle mask service on a farm box (owner 2026-10-15: «нужно разнести это всё на флот, чтобы не один только 4090
# участвовал в тестировании»). worker-4090 keeps its own masks-4090.ps1; this script is for every other box.
#
#   masks-box.ps1 -Mode Install -Box f12 -Python C:\AI\ComfyUI_windows_portable\python_embeded\python.exe [-Payload x.zip]
#   masks-box.ps1 -Mode Start|Stop|Status|Uninstall
#
# Install expands the payload (grounding-dino\, checkpoints\sam2.1_hiera_tiny.pt, sam2\, pylib\{hydra,iopath,portalocker},
# masks_service.py) into -Root, writes run-masks.cmd and registers the scheduled task «AutoRig Masks Service»
# (SYSTEM, at startup, restart on failure, no time limit) that runs the box's own torch-CUDA python with
# masks_service.py on 127.0.0.1:8989. The models are freed after -IdleUnload idle seconds (the card is shared with
# renders / Hunyuan) and reload on the next /masks. The VPS reaches the port through the box's SSH tunnel
# (deploy/vehicle/README.md, «Fleet mask service»).
param(
    [ValidateSet("Install", "Start", "Stop", "Status", "Uninstall")][string]$Mode = "Status",
    [string]$Root = "C:\ProgramData\AutoRig\masks",
    [string]$Python = "",
    [string]$Box = $env:COMPUTERNAME,
    [int]$Port = 8989,
    [int]$IdleUnload = 600,
    [string]$Payload = ""
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$TaskName = "AutoRig Masks Service"

function Health {
    try { return (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 5).Content } catch { return "" }
}

function Kill-Service {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -like "*masks_service.py*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "killed python $($_.ProcessId)" }
}

switch ($Mode) {
    "Install" {
        if (-not $Python -or -not (Test-Path $Python)) { throw "-Python must be the box's torch-CUDA python.exe" }
        New-Item -ItemType Directory -Force $Root, (Join-Path $Root "logs") | Out-Null
        if ($Payload) {
            Add-Type -AssemblyName System.IO.Compression.FileSystem
            $zip = [IO.Compression.ZipFile]::OpenRead($Payload)
            try {
                foreach ($e in $zip.Entries) {
                    if (-not $e.Name) { continue }
                    $dst = Join-Path $Root ($e.FullName -replace '/', '\')
                    New-Item -ItemType Directory -Force (Split-Path $dst) | Out-Null
                    [IO.Compression.ZipFileExtensions]::ExtractToFile($e, $dst + ".part", $true)
                    Move-Item -Force ($dst + ".part") $dst          # atomic per file
                }
            } finally { $zip.Dispose() }
        }
        foreach ($need in @("masks_service.py", "grounding-dino\model.safetensors", "checkpoints\sam2.1_hiera_tiny.pt", "sam2\build_sam.py")) {
            if (-not (Test-Path (Join-Path $Root $need))) { throw "missing $need under $Root" }
        }
        $log = Join-Path $Root "logs\masks.out.log"
        $cmd = @"
@echo off
rem AutoRig Masks Service on $Box (deploy/vehicle/masks-box.ps1). Loops forever; the task restarts it if cmd dies.
set PYTORCH_JIT=0
set HF_HUB_OFFLINE=1
set TRANSFORMERS_OFFLINE=1
set PYTHONIOENCODING=utf-8
set VEHICLE_MASKS_BOX=$Box
cd /d "$Root"
:loop
for %%F in ("$log") do if %%~zF GTR 5000000 move /y "$log" "$log.1" >nul
"$Python" -s "$Root\masks_service.py" --root "$Root" --port $Port --device cuda --idle-unload $IdleUnload --box $Box >> "$log" 2>&1
ping -n 11 127.0.0.1 >nul
goto loop
"@
        Set-Content -Path (Join-Path $Root "run-masks.cmd") -Value $cmd -Encoding ASCII
        $a = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$Root\run-masks.cmd`"" -WorkingDirectory $Root
        $t = New-ScheduledTaskTrigger -AtStartup
        $s = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
            -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew
        $p = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
        Register-ScheduledTask -TaskName $TaskName -Action $a -Trigger $t -Settings $s -Principal $p `
            -Description "Vehicles V3 mask service (GroundingDINO-tiny + SAM2.1-tiny) on 127.0.0.1:$Port; deploy/vehicle/masks-box.ps1" -Force | Out-Null
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Kill-Service
        Start-ScheduledTask -TaskName $TaskName
        "installed $TaskName on $Box (python $Python, root $Root, port $Port, idle-unload $IdleUnload s)"
        for ($i = 0; $i -lt 90; $i++) {
            $h = Health
            if ($h -and $h -match '"loaded": true') { "health: $h"; break }
            Start-Sleep -Seconds 2
        }
        if (-not ($h -match '"loaded": true')) { "health (not loaded yet): $h"; Get-Content $log -Tail 20 -ErrorAction SilentlyContinue }
    }
    "Start" { Start-ScheduledTask -TaskName $TaskName; Start-Sleep 5; "health: " + (Health) }
    "Stop" { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue; Kill-Service }
    "Uninstall" { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue; Kill-Service
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue; "task removed (files kept in $Root)" }
    "Status" {
        $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        "task:   " + $(if ($t) { $t.State } else { "not installed" })
        "health: " + (Health)
    }
}
