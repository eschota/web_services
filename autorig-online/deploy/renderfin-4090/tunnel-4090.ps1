$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ssh = "$env:WINDIR\System32\OpenSSH\ssh.exe"
$key = Join-Path $root "id_ed25519"
$knownHosts = Join-Path $root "known_hosts"
$statusLog = Join-Path $root "tunnel-status.log"

while ($true) {
    & $ssh `
        -N -T `
        -o BatchMode=yes `
        -o IdentitiesOnly=yes `
        -o ConnectTimeout=10 `
        -o ExitOnForwardFailure=yes `
        -o ServerAliveInterval=20 `
        -o ServerAliveCountMax=3 `
        -o StrictHostKeyChecking=yes `
        -o UserKnownHostsFile=$knownHosts `
        -i $key `
        -p 22744 `
        -R localhost:19409:127.0.0.1:8188 `
        debian@37.187.57.177

    $exitCode = $LASTEXITCODE
    Add-Content -LiteralPath $statusLog -Encoding UTF8 -Value "$(Get-Date -Format o) ssh_exit=$exitCode retry_seconds=10"
    if ((Get-Item -LiteralPath $statusLog -ErrorAction SilentlyContinue).Length -gt 131072) {
        Get-Content -LiteralPath $statusLog -Tail 200 | Set-Content -LiteralPath $statusLog -Encoding UTF8
    }
    Start-Sleep -Seconds 10
}
