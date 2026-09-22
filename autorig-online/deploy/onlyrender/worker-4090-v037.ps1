param([ValidateSet('Start','Stop','Status')][string]$Mode = 'Status')
$ErrorActionPreference = 'Stop'
$runtime = 'R:\autorig\.runtime\onlyrender'
$python = 'R:\ComfyUI_windows_portable\python_embeded\python.exe'
$env:CIVITAI_API_TOKEN = [Environment]::GetEnvironmentVariable('CIVITAI_API_TOKEN', 'User')
$launcher = "$runtime\launch.py"
$base = "$runtime\runtime"
$comfyUrl = 'http://127.0.0.1:8988'
$expectedCommit = 'e638023d54497dbe0579565e5de4bb7076899592'

function Set-Worker([hashtable]$Fields) {
    $Fields.render_server_name = 'worker-4090'
    $reply = ($Fields | ConvertTo-Json -Compress -Depth 8) | & ssh.exe -o ConnectTimeout=10 autorig-vps "curl -fsS --max-time 20 -X POST http://127.0.0.1:8210/renderfin/api-render -H 'Content-Type: application/json' --data-binary @-"
    if ($LASTEXITCODE -ne 0 -or -not ($reply | ConvertFrom-Json).ok) { throw 'Farm registry update failed' }
}
function Get-Comfy {
    try { Invoke-RestMethod "$comfyUrl/system_stats" -TimeoutSec 5 } catch { $null }
}
function Get-ListenerProcess {
    $connection = Get-NetTCPConnection -State Listen -LocalPort 8988 -ErrorAction SilentlyContinue
    if (-not $connection) { return $null }
    Get-CimInstance Win32_Process -Filter "ProcessId=$($connection[0].OwningProcess)"
}
function Test-OwnProcess($Process) {
    $Process -and $Process.ExecutablePath -eq $python -and
        $Process.CommandLine -match [regex]::Escape($launcher) -and
        $Process.CommandLine -match '(?:^|\s)--port\s+8988(?:\s|$)'
}
function Test-ComfyNode([string]$NodeName) {
    try { $info = Invoke-RestMethod "$comfyUrl/object_info/$NodeName" -TimeoutSec 10; return $null -ne $info.$NodeName } catch { return $false }
}

if ((& git.exe -C "$runtime\ComfyUI" rev-parse HEAD).Trim() -ne $expectedCommit) { throw 'OnlyRender ComfyUI commit mismatch' }
if (-not (Test-Path "$runtime\python-overlay\comfy_kitchen") -or -not (Test-Path "$runtime\python-overlay\comfy_aimdo")) { throw 'OnlyRender dependency overlay is incomplete' }

if ($Mode -eq 'Status') {
    $comfy = Get-Comfy
    $process = Get-ListenerProcess
    $queue = if ($comfy) { Invoke-RestMethod "$comfyUrl/queue" -TimeoutSec 10 } else { $null }
    [pscustomobject]@{
        ComfyVersion = $comfy.system.comfyui_version
        ProcessId = $process.ProcessId
        CommandLine = $process.CommandLine
        QueueRunning = @($queue.queue_running).Count
        QueuePending = @($queue.queue_pending).Count
        ReadyFlag = (Test-Path "$runtime\WAN2_PROMOTED")
    } | ConvertTo-Json -Depth 3
    & nvidia-smi --query-gpu=memory.used,memory.free --format=csv
    exit
}
if ($Mode -eq 'Stop') {
    Set-Worker @{render_operation='set_status';status='offline'}
    while (Get-Comfy) {
        $queue = Invoke-RestMethod "$comfyUrl/queue" -TimeoutSec 10
        if (-not $queue.queue_running.Count -and -not $queue.queue_pending.Count) { break }
        Write-Host 'Finishing accepted renders; v0.37 worker is offline for new work...'
        Start-Sleep -Seconds 5
    }
    $process = Get-ListenerProcess
    if ($process) {
        if (-not (Test-OwnProcess $process)) { throw 'Refusing to stop a non-v0.37 listener on port 8988' }
        Stop-Process -Id $process.ProcessId
    }
    Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | Where-Object {
        $_.CommandLine -match '19409:127\.0\.0\.1:8988' -and $_.CommandLine -match 'autorig-vps'
    } | ForEach-Object { Stop-Process -Id $_.ProcessId }
    exit
}

$process = Get-ListenerProcess
if ($process -and -not (Test-OwnProcess $process)) { throw 'Port 8988 belongs to another runtime; use its own controller to stop it first' }
if (-not $process) {
    $arguments = @(
        $launcher,'--listen','127.0.0.1','--port','8988',
        '--base-directory',$base,'--input-directory',"$base\input",'--output-directory',"$base\output",
        '--temp-directory',"$base\temp",'--user-directory',"$base\user",
        '--extra-model-paths-config',"$runtime\extra_model_paths.yaml"
    )
    Start-Process -FilePath $python -WindowStyle Hidden -WorkingDirectory 'R:\autorig' -ArgumentList $arguments -RedirectStandardOutput "$base\worker.log" -RedirectStandardError "$base\worker.err.log"
    for ($attempt=0; $attempt -lt 90 -and -not (Get-Comfy); $attempt++) { Start-Sleep -Seconds 2 }
    if (-not (Get-Comfy)) { throw 'ComfyUI v0.37 did not start; inspect worker.err.log' }
}
$tunnel = Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | Where-Object {
    $_.CommandLine -match '19409:127\.0\.0\.1:8988' -and $_.CommandLine -match 'autorig-vps'
}
if (-not $tunnel) {
    Start-Process ssh.exe -WindowStyle Hidden -ArgumentList @('-N','-R','19409:127.0.0.1:8988','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=30','-o','ServerAliveCountMax=3','autorig-vps')
    Start-Sleep -Seconds 3
}
& ssh.exe -o ConnectTimeout=10 autorig-vps 'curl -fsS --max-time 10 http://127.0.0.1:19409/system_stats >/dev/null'
if ($LASTEXITCODE -ne 0) { throw 'The VPS cannot reach the v0.37 worker' }

$workflows = @(
    'gen_animation_ltx23_by_url.json','gen_animation_ltx10eros_by_url.json',
    'gen_video_ltx23_control_by_url.json','gen_video_ltx23_pose_by_url.json','gen_video_ltx23_depth_by_url.json',
    'gen_image_flux2_klein.json','gen_image_flux2_klein_edit.json','gen_image_flux2_avatar.json',
    'gen_image_sdxl.json','gen_image_sdxl_edit.json',
    'gen_image_sdxl_control_pose.json','gen_image_sdxl_control_depth.json','gen_image_sdxl_control_canny.json',
    'gen_image.json','gen_image_flux1_schnell.json'
)
if (Test-Path "$runtime\WAN2_PROMOTED") {
    foreach ($node in 'WanAnimate2ToVideo','WanAnimate2Cache') { if (-not (Test-ComfyNode $node)) { throw "Missing promoted node $node" } }
    $workflows += 'gen_video_wan_animate2_by_url.json'
}
# LTX-2.5 weights live on C: (R: is full); extra_model_paths.yaml maps C:\AIModels.
$ltx25 = 'C:\AIModels'
if ((Test-Path "$ltx25\diffusion_models\ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors") -and
    (Test-Path "$ltx25\text_encoders\gemma4-12b-with-proj-ltx-2.5-comfy-int8-convrot.safetensors") -and
    (Test-Path "$ltx25\vae\ltx-2.5-video-vae-bf16.safetensors") -and
    (Test-Path "$ltx25\vae\ltx-2.5-audio-vae-bf16.safetensors")) {
    # The historical tokens run the same LTX-2.5 graphs since 2026-09-23.
    $workflows += @('gen_animation_ltx25_by_url.json', 'gen_animation_by_url.json')
    if (Test-Path "$ltx25\latent_upscale_models\ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors") {
        $workflows += @('gen_animation_ltx25_hq_by_url.json', 'gen_animation_hq_by_url.json')
    }
}
# MiniMax H3 (full int8-convrot transformer, nvfp4 Qwen3-VL encoder, 4-step turbo LoRA).
if ((Test-Path "$ltx25\diffusion_models\minimax_h3_fl2va_int8_convrot.safetensors") -and
    (Test-Path "$ltx25\text_encoders\qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors") -and
    (Test-Path "$ltx25\vae\minimax_h3_video_vae_fp16.safetensors") -and
    (Test-Path "$ltx25\vae\minimax_h3_audio_vae_fp32.safetensors") -and
    (Test-Path "$ltx25\loras\minimax_h3_fl2v_turbo_4step_v1.2_768p_comfyui_bf16.safetensors") -and
    (Test-ComfyNode 'MiniMaxH3ImageToVideo')) { $workflows += 'gen_video_minimax_h3_by_url.json' }
$overrides = @{'gen_image.json'='gen_image_flux1_schnell.json'}
Set-Worker @{render_operation='info';render_server_url='http://127.0.0.1:19409';gpu_name='RTX 4090';status='online';available_workflows=$workflows;workflow_overrides=$overrides;basic_auth=$false}
Write-Host ('OnlyRender v0.37 ready: ' + ($workflows -join ', '))
