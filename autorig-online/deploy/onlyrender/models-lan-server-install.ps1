$dir = 'C:\ProgramData\AutoRig'
$script = Join-Path $dir 'models_lan_server.ps1'
$name = 'AutoRig Models LAN Server'
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$script`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
# retire any manually started server on the same port, then start the task
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*http.server 18998*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; "stopped manual pid $($_.ProcessId)" }
Start-ScheduledTask -TaskName $name
Start-Sleep -Seconds 8
(Get-ScheduledTask -TaskName $name).State
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*http.server 18998*' } | ForEach-Object { "serving pid $($_.ProcessId): $($_.CommandLine)" }
