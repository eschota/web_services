param(
    [string]$ProbeUrl = 'https://autorig.online/gallery',
    [int]$FailureThreshold = 2,
    [int]$ProbeTimeoutSeconds = 15,
    [int]$RecoveryTimeoutSeconds = 90,
    [switch]$NoRestart
)

$ErrorActionPreference = 'Stop'
$stateRoot = 'C:\ProgramData\AutoRig\watchdogs\f13-egress'
$statePath = Join-Path $stateRoot 'state.json'
$logPath = Join-Path $stateRoot ("watchdog-{0}.log" -f (Get-Date -Format 'yyyy-MM-dd'))
$converterStatusUrl = 'http://127.0.0.1:5267/api-converter-glb/server-status'
$vpnServiceDisplayName = 'Adguard VPN Service'
$mutex = New-Object System.Threading.Mutex($false, 'Global\AutoRigF13EgressWatchdog')

function Write-WatchdogLog([string]$Message) {
    $line = "{0} {1}" -f (Get-Date).ToString('o'), $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding UTF8
    Write-Output $line
}

function Read-WatchdogState {
    if (-not (Test-Path -LiteralPath $statePath)) {
        return [pscustomobject]@{
            consecutive_failures = 0
            last_probe_at = $null
            last_success_at = $null
            last_failure_at = $null
            last_recovery_at = $null
            last_result = 'new'
        }
    }
    try {
        return Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    } catch {
        Write-WatchdogLog ("state_read_failed error={0}" -f $_.Exception.Message)
        return [pscustomobject]@{
            consecutive_failures = 0
            last_probe_at = $null
            last_success_at = $null
            last_failure_at = $null
            last_recovery_at = $null
            last_result = 'state_reset'
        }
    }
}

function Save-WatchdogState($State) {
    $temporaryPath = "$statePath.tmp"
    $State | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $temporaryPath -Encoding UTF8
    Move-Item -LiteralPath $temporaryPath -Destination $statePath -Force
}

function Test-AutoRigEgress {
    $curl = Join-Path $env:SystemRoot 'System32\curl.exe'
    $arguments = @(
        '--silent', '--show-error', '--location',
        '--connect-timeout', '5', '--max-time', [string]$ProbeTimeoutSeconds,
        '--output', 'NUL', '--write-out', '%{http_code}', $ProbeUrl
    )
    $result = & $curl @arguments 2>&1
    $exitCode = $LASTEXITCODE
    $statusCode = [string]$result
    return [pscustomobject]@{
        Healthy = ($exitCode -eq 0 -and $statusCode -match '^2[0-9][0-9]$')
        ExitCode = $exitCode
        StatusCode = $statusCode
    }
}

function Get-ConverterActivity {
    $status = Invoke-RestMethod -Uri $converterStatusUrl -TimeoutSec 15
    $hunyuanActive = $false
    if ($null -ne $status.hunyuan -and $null -ne $status.hunyuan.active_task) {
        $hunyuanActive = -not [string]::IsNullOrWhiteSpace([string]$status.hunyuan.active_task)
    }
    return [pscustomobject]@{
        ProcessId = [int]$status.process_id
        Processing = [int]$status.tasks_summary.processing
        Pending = [int]$status.tasks_summary.pending
        HunyuanActive = $hunyuanActive
        Idle = ([int]$status.tasks_summary.processing -eq 0 -and [int]$status.tasks_summary.pending -eq 0 -and -not $hunyuanActive)
    }
}

New-Item -ItemType Directory -Force -Path $stateRoot | Out-Null
$lockTaken = $false
try {
    $lockTaken = $mutex.WaitOne(0)
    if (-not $lockTaken) {
        Write-WatchdogLog 'skip reason=already_running'
        exit 0
    }

    $state = Read-WatchdogState
    $now = (Get-Date).ToString('o')
    $state.last_probe_at = $now
    $probe = Test-AutoRigEgress
    if ($probe.Healthy) {
        $state.consecutive_failures = 0
        $state.last_success_at = $now
        $state.last_result = "healthy_http_$($probe.StatusCode)"
        Save-WatchdogState $state
        Write-WatchdogLog ("healthy http={0}" -f $probe.StatusCode)
        exit 0
    }

    $state.consecutive_failures = [int]$state.consecutive_failures + 1
    $state.last_failure_at = $now
    $state.last_result = "probe_failed_exit_$($probe.ExitCode)_http_$($probe.StatusCode)"
    Save-WatchdogState $state
    Write-WatchdogLog ("probe_failed count={0} exit={1} http={2}" -f $state.consecutive_failures, $probe.ExitCode, $probe.StatusCode)

    if ([int]$state.consecutive_failures -lt $FailureThreshold) {
        exit 1
    }
    if ($NoRestart) {
        Write-WatchdogLog 'recovery_skipped reason=no_restart'
        exit 2
    }

    try {
        $activity = Get-ConverterActivity
    } catch {
        Write-WatchdogLog ("recovery_deferred reason=converter_status_unavailable error={0}" -f $_.Exception.Message)
        exit 3
    }
    if (-not $activity.Idle) {
        Write-WatchdogLog ("recovery_deferred reason=converter_busy pid={0} processing={1} pending={2} hunyuan_active={3}" -f $activity.ProcessId, $activity.Processing, $activity.Pending, $activity.HunyuanActive)
        exit 4
    }

    Write-WatchdogLog ("recovery_start action=restart_adguard_vpn converter_pid={0}" -f $activity.ProcessId)
    Restart-Service -DisplayName $vpnServiceDisplayName -Force
    $deadline = (Get-Date).AddSeconds($RecoveryTimeoutSeconds)
    do {
        Start-Sleep -Seconds 3
        $recoveryProbe = Test-AutoRigEgress
        if ($recoveryProbe.Healthy) {
            $recoveredAt = (Get-Date).ToString('o')
            $state.consecutive_failures = 0
            $state.last_success_at = $recoveredAt
            $state.last_recovery_at = $recoveredAt
            $state.last_result = "recovered_http_$($recoveryProbe.StatusCode)"
            Save-WatchdogState $state
            Write-WatchdogLog ("recovery_ok http={0} converter_pid={1}" -f $recoveryProbe.StatusCode, $activity.ProcessId)
            exit 0
        }
    } while ((Get-Date) -lt $deadline)

    $state.last_result = 'recovery_timeout'
    Save-WatchdogState $state
    Write-WatchdogLog 'recovery_failed reason=timeout'
    exit 5
} finally {
    if ($lockTaken) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
