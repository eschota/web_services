# Register (or re-register) the scheduled task that keeps the 1080 Ti image
# worker running: at startup, as the converter's own account, S4U (no stored
# password), below-normal priority so conversions keep the CPU. -Start runs it
# now; -Remove unregisters it and stops both processes.
param([switch]$Start, [switch]$Remove, [string]$User = "$env:COMPUTERNAME\user")
$name = 'AutoRig Comfy1080'
$state = 'C:\ProgramData\AutoRig\comfy1080'
function Stop-Worker {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like '*comfy1080_guard.py*' -or ($_.ExecutablePath -like 'C:\AI\ComfyUI1080\*') } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" |
        Where-Object { $_.CommandLine -like '*comfy1080-run.ps1*' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}
if ($Remove) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Stop-Worker
    'removed'
    exit 0
}
# The installer ran as SYSTEM; the worker account needs to write outputs/logs.
foreach ($dir in 'C:\AI\ComfyUI1080', $state) {
    & icacls.exe $dir /grant "${User}:(OI)(CI)M" /T /C /Q | Out-Null
}
$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File $state\comfy1080-run.ps1"
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = 'PT2M'
$principal = New-ScheduledTaskPrincipal -UserId $User -LogonType S4U -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -Priority 7
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
"registered $name as $User"
if ($Start) { Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue; Stop-Worker; Start-Sleep -Seconds 3; Start-ScheduledTask -TaskName $name; 'started' }
