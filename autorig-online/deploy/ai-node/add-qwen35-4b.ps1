# Add Qwen3.5-4B as a second model on one Windows ai-node (the 2026-11-06 A/B), from the box itself.
# The OWNER runs this (farm rule: models are installed by the owner).
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File add-qwen35-4b.ps1 -Task "AutoRig AI Node F13" [-Download] [-NewCode <ai_node.py>]
#
# 1. -Download: huggingface-cli download unsloth/Qwen3.5-4B-GGUF Qwen3.5-4B-Q4_K_M.gguf mmproj-F16.gguf
#    into <models>\qwen35-4b (public repo, ~3.4 GB; HF_TOKEN from the env is used if set). Without -Download the
#    two files must already be there (e.g. copied over the LAN from a box that has them).
# 2. Checks the sizes against the Hugging Face listing of 2026-11-06.
# 3. Backs up config.json (*.bak-4b-<time>) and adds a "models" entry for qwen35-4b with its own llama-server
#    flags (validated against llama.cpp b10709 on f13; no -np 2: parallel vision crashed b10709 on f2, see README). The 9B stays the primary model and keeps its flags.
#    Needs an ai_node.py with the "models" key (ai-node-20261106-models or later): pass it with -NewCode.
# 4. Restarts the node's scheduled task once it has no AI task (apply-node-tuning.ps1 does the same wait).
# Rollback: copy the backup over config.json (and ai_node.py.bak-4b-*) and restart the task.
param(
    [Parameter(Mandatory = $true)][string]$Task,
    [string]$Dir = 'C:\ProgramData\AutoRig\ai-node',
    [string]$ModelsDir = '',
    [string]$NewCode = '',
    [switch]$Download,
    [string[]]$LlamaArgs = @('-b', '2048', '-ub', '1024', '-fa', 'on', '-ctk', 'q8_0', '-ctv', 'q8_0',
                             '--image-min-tokens', '64', '--image-max-tokens', '1024'),
    [int]$VramNeedMb = 5600
)
$ErrorActionPreference = 'Stop'
$cfgPath = Join-Path $Dir 'config.json'
$cfg = Get-Content $cfgPath -Raw | ConvertFrom-Json
if (-not $ModelsDir) { $ModelsDir = Split-Path -Parent $cfg.weights }
$target = Join-Path $ModelsDir 'qwen35-4b'
$files = @{ 'Qwen3.5-4B-Q4_K_M.gguf' = 2740937888; 'mmproj-F16.gguf' = 672423616 }

if ($Download) {
    New-Item -ItemType Directory -Force $target | Out-Null
    $cli = Get-Command huggingface-cli -ErrorAction SilentlyContinue
    if ($cli) {
        & $cli.Source download unsloth/Qwen3.5-4B-GGUF Qwen3.5-4B-Q4_K_M.gguf mmproj-F16.gguf --local-dir $target
    } else {
        $hf = Get-Command hf -ErrorAction SilentlyContinue
        if (-not $hf) { throw 'neither huggingface-cli nor hf found: pip install -U "huggingface_hub[cli]"' }
        & $hf.Source download unsloth/Qwen3.5-4B-GGUF Qwen3.5-4B-Q4_K_M.gguf mmproj-F16.gguf --local-dir $target
    }
}
foreach ($name in $files.Keys) {
    $path = Join-Path $target $name
    if (-not (Test-Path $path)) { throw "missing $path" }
    $size = (Get-Item $path).Length
    if ($size -ne $files[$name]) { throw "$name is $size bytes, expected $($files[$name])" }
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
Copy-Item $cfgPath "$cfgPath.bak-4b-$stamp"
if ($NewCode) {
    Copy-Item (Join-Path $Dir 'ai_node.py') (Join-Path $Dir "ai_node.py.bak-4b-$stamp")
}
$entry = [pscustomobject]@{
    model_id         = 'qwen35-4b'
    weights          = (Join-Path $target 'Qwen3.5-4B-Q4_K_M.gguf')
    mmproj           = (Join-Path $target 'mmproj-F16.gguf')
    context_tokens   = 4096
    reasoning        = 'off'
    vram_need_mb     = $VramNeedMb
    extra_llama_args = $LlamaArgs
}
$others = @($cfg.models | Where-Object { $_ -and $_.model_id -ne 'qwen35-4b' })
$cfg | Add-Member -NotePropertyName models -NotePropertyValue (@($others) + $entry) -Force

# A restart forgets in-memory tasks: wait until the node is idle.
$token = (Get-Content (Join-Path $Dir $cfg.token_file) -Raw).Trim()
$base = "http://127.0.0.1:$($cfg.listen_port)/api-converter-glb"
$deadline = (Get-Date).AddMinutes(15)
do {
    $s = Invoke-RestMethod -Uri "$base/server-status" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 10
    $load = [int]$s.tasks_summary.processing + [int]$s.tasks_summary.pending
    if ($load -eq 0) { break }
    if ((Get-Date) -gt $deadline) { throw "node still busy after 15 min; config not changed" }
    Start-Sleep 2
} while ($true)
if ($NewCode) { Copy-Item $NewCode (Join-Path $Dir 'ai_node.py') -Force }
[IO.File]::WriteAllText($cfgPath, ($cfg | ConvertTo-Json -Depth 6), (New-Object Text.UTF8Encoding $false))
Stop-ScheduledTask -TaskName $Task
Start-Sleep 3
Start-ScheduledTask -TaskName $Task
Start-Sleep 8
$s = Invoke-RestMethod -Uri "$base/server-status" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 10
Write-Output ("version={0} models={1}" -f $s.server_version, (($s.ai_models.models | ForEach-Object { $_.id }) -join ','))
