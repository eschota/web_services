param(
    [ValidateSet('Start','Stop','Status')][string]$Mode = 'Status',
    [switch]$Legacy
)
$ErrorActionPreference = 'Stop'
$promotionRoot = 'R:\autorig\.runtime\onlyrender'
$promotedController = 'R:\autorig\autorig-online\deploy\onlyrender\worker-4090-v037.ps1'
if (-not $Legacy -and (Test-Path "$promotionRoot\WAN2_PROMOTED")) {
    if (-not (Test-Path $promotedController)) { throw 'Promoted OnlyRender controller is missing' }
    & $promotedController -Mode $Mode
    exit $LASTEXITCODE
}
$root = 'R:\ComfyUI_windows_portable'
$env:CIVITAI_API_TOKEN = [Environment]::GetEnvironmentVariable('CIVITAI_API_TOKEN', 'User')
$comfyUrl = 'http://127.0.0.1:8988'

function Set-Worker([hashtable]$Fields) {
    $Fields.render_server_name = 'worker-4090'
    $reply = ($Fields | ConvertTo-Json -Compress -Depth 8) | & ssh.exe -o ConnectTimeout=10 autorig-vps "curl -fsS --max-time 20 -X POST http://127.0.0.1:8210/renderfin/api-render -H 'Content-Type: application/json' --data-binary @-"
    if ($LASTEXITCODE -ne 0 -or -not ($reply | ConvertFrom-Json).ok) { throw 'Farm registry update failed' }
}
function Get-Comfy {
    try { Invoke-RestMethod "$comfyUrl/system_stats" -TimeoutSec 5 } catch { $null }
}
function Test-ComfyNode([string]$NodeName) {
    try {
        $info = Invoke-RestMethod "$comfyUrl/object_info/$NodeName" -TimeoutSec 10
        return $null -ne $info.$NodeName
    } catch { return $false }
}

if ($Mode -eq 'Status') {
    Get-Comfy | ConvertTo-Json -Depth 6
    & nvidia-smi --query-gpu=memory.used,memory.free --format=csv
    exit
}
if ($Mode -eq 'Stop') {
    # Drain accepted work before returning the GPU to its owner.
    Set-Worker @{render_operation='set_status';status='offline'}
    while (Get-Comfy) {
        $queue = Invoke-RestMethod "$comfyUrl/queue" -TimeoutSec 10
        if (-not $queue.queue_running.Count -and -not $queue.queue_pending.Count) { break }
        Write-Host 'Finishing accepted renders; worker is offline for new work...'
        Start-Sleep -Seconds 5
    }
    $connection = Get-NetTCPConnection -State Listen -LocalPort 8988 -ErrorAction SilentlyContinue
    foreach ($procId in @($connection.OwningProcess | Sort-Object -Unique)) {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$procId"
        if ($proc.ExecutablePath -eq "$root\python_embeded\python.exe" -and $proc.CommandLine -match 'ComfyUI\\main.py') {
            Stop-Process -Id $procId
        }
    }
    Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | Where-Object {
        $_.CommandLine -match '19409:127\.0\.0\.1:8988' -and $_.CommandLine -match 'autorig-vps'
    } | ForEach-Object { Stop-Process -Id $_.ProcessId }
    & nvidia-smi --query-gpu=memory.used,memory.free --format=csv
    exit
}

if (-not (Get-Comfy)) {
    Start-Process -FilePath "$root\python_embeded\python.exe" -WindowStyle Hidden -WorkingDirectory $root -ArgumentList @('-s','ComfyUI\main.py','--windows-standalone-build','--listen','127.0.0.1','--port','8988') -RedirectStandardOutput "$root\comfy_worker.log" -RedirectStandardError "$root\comfy_worker.err.log"
    $ready = $false
    for ($attempt=0; $attempt -lt 60; $attempt++) {
        if (Get-Comfy) { $ready=$true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) { throw 'ComfyUI did not start; inspect comfy_worker.err.log' }
}
$tunnel = Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" | Where-Object {
    $_.CommandLine -match '19409:127\.0\.0\.1:8988' -and $_.CommandLine -match 'autorig-vps'
}
if (-not $tunnel) {
    Start-Process ssh.exe -WindowStyle Hidden -ArgumentList @('-N','-R','19409:127.0.0.1:8988','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=30','-o','ServerAliveCountMax=3','autorig-vps')
    Start-Sleep -Seconds 3
}
& ssh.exe -o ConnectTimeout=10 autorig-vps 'curl -fsS --max-time 10 http://127.0.0.1:19409/system_stats >/dev/null'
if ($LASTEXITCODE -ne 0) { throw 'The VPS cannot reach this worker' }
$workflows = @()
if ((Test-Path "$root\ComfyUI\models\diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors") -and
    (Test-Path "$root\ComfyUI\models\text_encoders\ltx-2.3_text_projection_bf16.safetensors") -and
    (Test-Path "$root\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors") -and
    (Test-Path "$root\ComfyUI\models\vae\LTX23_video_vae_bf16.safetensors") -and
    (Test-Path "$root\ComfyUI\models\vae\LTX23_audio_vae_bf16.safetensors")) { $workflows += 'gen_animation_ltx23_by_url.json' }
if ((Test-Path "$root\ComfyUI\models\checkpoints\ltx10eros_v14_2989669.safetensors") -and
    (Test-Path "$root\ComfyUI\models\text_encoders\gemma_3_12B_it_fp4_mixed.safetensors")) { $workflows += 'gen_animation_ltx10eros_by_url.json' }
if (($workflows -contains 'gen_animation_ltx23_by_url.json') -and
    (Test-Path "$root\ComfyUI\models\loras\ltx-2.3-22b-ic-lora-union-control-ref0.5.safetensors") -and
    (Test-ComfyNode 'LTXAddVideoICLoRAGuide')) {
    $workflows += 'gen_video_ltx23_control_by_url.json'
    if ((Test-ComfyNode 'DWPreprocessor') -and
        (Test-Path "$root\ComfyUI\custom_nodes\comfyui_controlnet_aux\ckpts\yzd-v\DWPose\yolox_l.onnx") -and
        (Test-Path "$root\ComfyUI\custom_nodes\comfyui_controlnet_aux\ckpts\hr16\DWPose-TorchScript-BatchSize5\dw-ll_ucoco_384_bs5.torchscript.pt")) { $workflows += 'gen_video_ltx23_pose_by_url.json' }
    if ((Test-ComfyNode 'VideoDepthAnythingProcess') -and
        (Test-Path "$root\ComfyUI\models\videodepthanything\video_depth_anything_vits.pth")) { $workflows += 'gen_video_ltx23_depth_by_url.json' }
}
if ((Test-Path "$root\ComfyUI\models\diffusion_models\flux-2-klein-4b.safetensors") -and
    (Test-Path "$root\ComfyUI\models\text_encoders\qwen_3_4b_fp4_flux2.safetensors") -and
    (Test-Path "$root\ComfyUI\models\vae\flux2-vae.safetensors")) {
    $workflows += @('gen_image_flux2_klein.json','gen_image_flux2_klein_edit.json')
    if (Test-ComfyNode 'ReferenceLatent') { $workflows += 'gen_image_flux2_avatar.json' }
}
if (Test-Path "$root\ComfyUI\models\checkpoints\CyberRealisticPony_V18.0_F16.safetensors") { $workflows += @('gen_image_sdxl.json','gen_image_sdxl_edit.json') }
if ((Test-Path "$root\ComfyUI\models\checkpoints\CyberRealisticPony_V18.0_F16.safetensors") -and
    (Test-Path "$root\ComfyUI\models\controlnet\xinsir-controlnet-union-sdxl-1.0.safetensors")) {
    $workflows += @('gen_image_sdxl_control_pose.json','gen_image_sdxl_control_depth.json','gen_image_sdxl_control_canny.json')
}
# Image weights added after R: filled up live on C: (extra_model_paths.yaml
# base C:/AIModels); a file counts wherever ComfyUI can see it.
function Has-Model([string]$rel) {
    (Test-Path "$root\ComfyUI\models\$rel") -or (Test-Path "C:\AIModels\$rel")
}
# Z-Image Turbo fast tier. gen_image.json is also the T-pose/legacy-mode token,
# and those graphs need RMBG, which this box does not carry, so only the
# ControlNet tokens are claimed unless RMBG is present. The canny token is left
# out on purpose: upscale/detail/face-fix and Qwen-Image jobs are scheduled on it
# and need ESRGAN, TiledDiffusion and GGUF weights this box lacks.
$zimage = (Has-Model 'diffusion_models\z_image_turbo_fp8_e4m3fn.safetensors') -and
    (Has-Model 'text_encoders\qwen_3_4b.safetensors') -and (Has-Model 'vae\ae.safetensors') -and
    (Has-Model 'model_patches\Z-Image-Turbo-Fun-Controlnet-Union-2.1-2602-8steps.safetensors')
if ($zimage) {
    $workflows += @('gen_image_control_pose.json','gen_image_control_depth.json')
    if ((Test-ComfyNode 'RMBG') -and (Test-ComfyNode 'easy cleanGpuUsed')) { $workflows += 'gen_image.json' }
}
# Krea 2 Turbo quality tier.
if ((Has-Model 'diffusion_models\krea2_turbo_fp8_scaled.safetensors') -and
    (Has-Model 'text_encoders\qwen3vl_4b_fp8_scaled.safetensors') -and
    (Has-Model 'vae\qwen_image_vae.safetensors')) { $workflows += 'gen_image_krea2.json' }
if (-not $workflows.Count) { throw 'No complete current model set is installed' }
$overrides=@{}
# info preserves worker history, unlike add_server.
Set-Worker @{render_operation='info';render_server_url='http://127.0.0.1:19409';gpu_name='RTX 4090';status='online';available_workflows=$workflows;workflow_overrides=$overrides;basic_auth=$false}
Write-Host ('OnlyRender ready: ' + ($workflows -join ', '))
