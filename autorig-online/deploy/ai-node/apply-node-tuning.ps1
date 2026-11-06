# Apply (or roll back) the 2026-11-06 throughput tuning on one Windows ai-node box, from the box itself.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File apply-node-tuning.ps1 -Task "AutoRig AI Node F13" [-NewCode <path>] [-Rollback]
#
# Apply: backs up ai_node.py and config.json (*.bak-vab-20261106, once), optionally installs a new ai_node.py,
# sets parallel_tasks=2 and the llama-server flags below in config.json, waits until the node has no AI task
# (processing or pending, up to 15 min), then restarts the node's scheduled task (llama-server dies with it via
# the kill-on-close job) and waits for /healthz. Rollback: restores both backups and restarts the same way.
# The token is read from the node's token file and never printed.
param(
    [Parameter(Mandatory = $true)][string]$Task,
    [string]$Dir = 'C:\ProgramData\AutoRig\ai-node',
    [string]$NewCode = '',
    [string[]]$LlamaArgs = @('-fa', 'on', '-ctk', 'q8_0', '-ctv', 'q8_0', '-np', '2', '-kvu'),
    [int]$Parallel = 2,
    [int]$Keepalive = 600,
    [switch]$Rollback
)
$ErrorActionPreference = 'Stop'
$cfgPath = Join-Path $Dir 'config.json'
$codePath = Join-Path $Dir 'ai_node.py'
$suffix = '.bak-vab-20261106'
$cfg = Get-Content $cfgPath -Raw | ConvertFrom-Json
$token = (Get-Content (Join-Path $Dir $cfg.token_file) -Raw).Trim()
$base = "http://127.0.0.1:$($cfg.listen_port)/api-converter-glb"

function Get-Load {
    $s = Invoke-RestMethod -Uri "$base/server-status" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 10
    return [int]$s.tasks_summary.processing + [int]$s.tasks_summary.pending
}

if ($Rollback) {
    foreach ($p in @($cfgPath, $codePath)) {
        if (-not (Test-Path ($p + $suffix))) { throw "no backup $p$suffix" }
    }
} else {
    foreach ($p in @($cfgPath, $codePath)) {
        if (-not (Test-Path ($p + $suffix))) { Copy-Item $p ($p + $suffix) }
    }
}

# Wait for an idle node: a restart forgets in-memory tasks.
$deadline = (Get-Date).AddMinutes(15)
while ($true) {
    $load = Get-Load
    if ($load -eq 0) { break }
    if ((Get-Date) -gt $deadline) { throw "node still busy ($load task(s)) after 15 min; nothing changed" }
    Start-Sleep -Seconds 2
}

if ($Rollback) {
    Copy-Item ($cfgPath + $suffix) $cfgPath -Force
    Copy-Item ($codePath + $suffix) $codePath -Force
} else {
    if ($NewCode) { Copy-Item $NewCode $codePath -Force }
    $cfg | Add-Member -NotePropertyName parallel_tasks -NotePropertyValue $Parallel -Force
    $cfg.extra_llama_args = $LlamaArgs
    $cfg.keepalive_seconds = $Keepalive
    $json = $cfg | ConvertTo-Json -Depth 6
    [IO.File]::WriteAllText($cfgPath, $json, (New-Object Text.UTF8Encoding $false))
}

if ((Get-Load) -ne 0) { Write-Output 'a task arrived during the edit; restarting anyway after it finishes'; while ((Get-Load) -ne 0) { Start-Sleep 2 } }
Stop-ScheduledTask -TaskName $Task
Start-Sleep -Seconds 3
Start-ScheduledTask -TaskName $Task
$ok = $false
for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 1
    try { if ((Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$($cfg.listen_port)/healthz" -TimeoutSec 3).StatusCode -eq 200) { $ok = $true; break } } catch {}
}
$s = Invoke-RestMethod -Uri "$base/server-status" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 10
Write-Output ("healthz={0} version={1} accepting={2} models={3}" -f $ok, $s.server_version, $s.accepting_ai_vision, (($s.ai_models.models | ForEach-Object { $_.id }) -join ','))
