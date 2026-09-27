# Starts the worker-4090 3D adapter and its reverse tunnel if they are not running.
# Idempotent; run at logon by the scheduled task "AutoRig 4090 3D Adapter".
$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$py = 'C:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone\python.exe'
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
$adapter = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -match 'fleet-adapter\\adapter\.py' }
if (-not $adapter) {
    Start-Process $py -ArgumentList @('-u', "$here\adapter.py") -WorkingDirectory $here -WindowStyle Hidden `
        -RedirectStandardOutput "$here\stdout.log" -RedirectStandardError "$here\stderr.log"
}
$tunnel = Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | Where-Object { $_.CommandLine -match '15409:127\.0\.0\.1:18777' }
if (-not $tunnel) {
    Start-Process ssh.exe -WindowStyle Hidden -ArgumentList @('-N','-R','15409:127.0.0.1:18777','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=30','-o','ServerAliveCountMax=3','autorig-vps')
}
