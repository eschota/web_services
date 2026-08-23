param(
    [string]$SourceScript = (Join-Path $PSScriptRoot 'f13-egress-watchdog.ps1')
)

$ErrorActionPreference = 'Stop'
$taskPath = '\AutoRig\'
$taskName = 'F13 Egress Watchdog'
$destinationRoot = 'C:\ProgramData\AutoRig\watchdogs'
$destinationScript = Join-Path $destinationRoot 'f13-egress-watchdog.ps1'
$backupRoot = 'C:\ProgramData\AutoRig\backups'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Administrator privileges are required.'
}
if (-not (Test-Path -LiteralPath $SourceScript -PathType Leaf)) {
    throw "Watchdog source script not found: $SourceScript"
}

New-Item -ItemType Directory -Force -Path $destinationRoot | Out-Null
New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null

if (Test-Path -LiteralPath $destinationScript) {
    Copy-Item -LiteralPath $destinationScript -Destination (Join-Path $backupRoot ("f13-egress-watchdog-before-{0}.ps1" -f $stamp))
}
$existingTask = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction SilentlyContinue
if ($null -ne $existingTask) {
    Export-ScheduledTask -TaskPath $taskPath -TaskName $taskName |
        Set-Content -LiteralPath (Join-Path $backupRoot ("f13-egress-watchdog-task-before-{0}.xml" -f $stamp)) -Encoding Unicode
}

Copy-Item -LiteralPath $SourceScript -Destination $destinationScript -Force
$expectedHash = (Get-FileHash -LiteralPath $SourceScript -Algorithm SHA256).Hash
$actualHash = (Get-FileHash -LiteralPath $destinationScript -Algorithm SHA256).Hash
if ($actualHash -ne $expectedHash) {
    throw "Installed watchdog hash mismatch: expected $expectedHash, got $actualHash"
}

$powerShellExe = Join-Path $PSHOME 'powershell.exe'
$action = New-ScheduledTaskAction -Execute $powerShellExe -Argument ("-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"{0}`"" -f $destinationScript)
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 3)
$taskPrincipal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskPath $taskPath -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $taskPrincipal -Force | Out-Null

$probeProcess = Start-Process -FilePath $powerShellExe -ArgumentList @(
    '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $destinationScript
) -Wait -PassThru -NoNewWindow
if ($probeProcess.ExitCode -ne 0) {
    throw "Installed watchdog initial probe failed with exit code $($probeProcess.ExitCode)"
}

$installedTask = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName
$taskInfo = Get-ScheduledTaskInfo -TaskPath $taskPath -TaskName $taskName
[pscustomobject]@{
    TaskPath = $installedTask.TaskPath
    TaskName = $installedTask.TaskName
    State = $installedTask.State
    NextRunTime = $taskInfo.NextRunTime
    LastTaskResult = $taskInfo.LastTaskResult
    Script = $destinationScript
    Sha256 = $actualHash
}
