# Farm-box workflow advertiser (f5 / f15).
# Renderfin only dispatches a workflow to a box whose registry entry lists that
# file name, and these boxes run no agent of their own: the list was set by hand
# once and then silently outlived the models behind it. This does for them what
# deploy/onlyrender/worker-4090.ps1 does for the 4090 - derive the list from the
# files actually on disk - and posts it to the public renderfin API, which is
# the only route these boxes have to the VPS.
# Additive by design: a token this script does not know about is preserved, so
# a list another operator set is never clobbered.
param([string]$NodeName = $env:COMPUTERNAME, [switch]$WhatIfOnly, [string]$Models = '')
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
# f5/f15/Raptor keep ComfyUI on D:, f12 on C:\AI.
$models = $Models
if (-not $models) {
    $models = @('D:\ComfyUI_windows_portable\ComfyUI\models', 'C:\AI\ComfyUI_windows_portable\ComfyUI\models') |
        Where-Object { Test-Path $_ } | Select-Object -First 1
}
$api = 'https://autorig.online/renderfin/api-render'
$log = 'C:\ProgramData\AutoRig\fleet-ltx23\advertise.log'
$blockFile = 'C:\ProgramData\AutoRig\fleet-ltx23\advertise_block.txt'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
if ($NodeName -match '^F5') { $NodeName = 'f5' } elseif ($NodeName -match '^F15') { $NodeName = 'f15' }
elseif ($NodeName -match '^WIN-HGEREJIMQDO') { $NodeName = 'f12' } elseif ($NodeName -match '^RYZEN-SERVER') { $NodeName = 'Raptor' }
function Log($m) { Add-Content -Path $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) -Encoding utf8 }
function Has([string]$rel) { Test-Path (Join-Path $models $rel) }

$tokens = @()
# Video is LTX-2.5 distilled (int8-convrot transformer, Gemma 4 encoder with
# the LTX projection, 2.5 VAEs). The historical tokens keep their names so
# saved graphs and callers do not break: gen_animation_by_url (default video),
# gen_animation_ltx23_by_url and gen_animation_ltx10eros_by_url all run the
# single-stage graph, gen_animation_hq_by_url the two-stage graph, which also
# needs the x2 latent upscaler. The LTX 2.3 / LTX-2 19B / LTXV 13B files no
# longer advertise anything.
if ((Has 'diffusion_models\ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors') -and
    (Has 'text_encoders\gemma4-12b-with-proj-ltx-2.5-comfy-int8-convrot.safetensors') -and
    (Has 'vae\ltx-2.5-video-vae-bf16.safetensors') -and
    (Has 'vae\ltx-2.5-audio-vae-bf16.safetensors')) {
    $tokens += @('gen_animation_ltx25_by_url.json', 'gen_animation_by_url.json',
                 'gen_animation_ltx23_by_url.json', 'gen_animation_ltx10eros_by_url.json')
    if (Has 'latent_upscale_models\ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors') {
        $tokens += @('gen_animation_ltx25_hq_by_url.json', 'gen_animation_hq_by_url.json')
    }
}
# MiniMax H3 (full int8-convrot, nvfp4 encoder, turbo LoRA). Needs 64 GB RAM.
if ((Has 'diffusion_models\minimax_h3_fl2va_pruned_int8_convrot.safetensors') -and
    (Has 'text_encoders\qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors') -and
    (Has 'vae\minimax_h3_video_vae_fp16.safetensors') -and
    (Has 'vae\minimax_h3_audio_vae_fp32.safetensors') -and
    (Has 'loras\minimax_h3_fl2v_turbo_4step_v1.2_768p_comfyui_bf16.safetensors')) { $tokens += 'gen_video_minimax_h3_by_url.json' }
# FLUX.2 klein image pair.
if ((Has 'diffusion_models\flux-2-klein-4b.safetensors') -and
    (Has 'text_encoders\qwen_3_4b_fp4_flux2.safetensors') -and
    (Has 'vae\flux2-vae.safetensors')) { $tokens += @('gen_image_flux2_klein.json','gen_image_flux2_klein_edit.json') }
# SDXL/Pony (CyberRealistic Pony) was retired on 2026-09-23: gen_image_sdxl.json
# is no longer advertised; list it in advertise_block.txt to strip an old entry.
# Z-Image Turbo is the fast image tier: plain text-to-image, the T-pose render
# and legacy typed modes all schedule as gen_image.json, and the Fun ControlNet
# Union patch serves pose/depth/canny/inpaint. The enhancement and Qwen-Image
# jobs ride the canny token, so it needs the same files.
if ((Has 'diffusion_models\z_image_turbo_fp8_e4m3fn.safetensors') -and
    (Has 'text_encoders\qwen_3_4b.safetensors') -and
    (Has 'vae\ae.safetensors') -and
    (Has 'model_patches\Z-Image-Turbo-Fun-Controlnet-Union-2.1-2602-8steps.safetensors')) {
    $tokens += @('gen_image.json','gen_image_control_pose.json','gen_image_control_depth.json','gen_image_control_canny.json')
}
# ControlNet maps (the /api/controlnet jobs): only the preprocessor node pack is
# needed; its annotator weights download on first use. Until 2026-09-26 only f12
# advertised these, and with f12 offline every map job sat blocked.
if (Test-Path (Join-Path (Split-Path $models) 'custom_nodes\comfyui_controlnet_aux')) {
    $tokens += @('gen_control_depth.json','gen_control_pose.json','gen_control_canny.json')
}
# Krea 2 Turbo is the quality tier with the live LoRA ecosystem.
if ((Has 'diffusion_models\krea2_turbo_fp8_scaled.safetensors') -and
    (Has 'text_encoders\qwen3vl_4b_fp8_scaled.safetensors') -and
    (Has 'vae\qwen_image_vae.safetensors')) { $tokens += 'gen_image_krea2.json' }

# Having the files is not the same as being able to run them: measurement
# settles that. Names listed one per line in advertise_block.txt are stripped
# from the list instead of added to it, so a box that crashes or is hopelessly
# slow on a workflow stays out of its rota across reboots and across every
# later run of this script.
$blocked = @()
if (Test-Path $blockFile) {
    $blocked = @(Get-Content $blockFile | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' -and -not $_.StartsWith('#') })
}
$tokens = @($tokens | Where-Object { $blocked -notcontains $_ })
$msg = "$NodeName computed: " + ($tokens -join ', ')
if ($blocked.Count -gt 0) { $msg = $msg + ' | blocked: ' + ($blocked -join ', ') }
Log $msg

try { $dash = Invoke-RestMethod -Uri $api -TimeoutSec 30 }
catch { Log ('dashboard read failed: ' + $_.Exception.Message); exit 1 }
$me = $dash.servers | Where-Object { $_.render_server_name -eq $NodeName }
if (-not $me) { Log "$NodeName is not registered; refusing to invent an entry"; exit 1 }
$have = @($me.available_workflows)
$keep = @($have | Where-Object { $blocked -notcontains $_ })
$add = @($tokens | Where-Object { $keep -notcontains $_ })
$new = @($keep + $add)
if (-not (Compare-Object $new $have)) { Log "$NodeName already advertises exactly what it should"; exit 0 }
$body = @{ render_server_name = $NodeName; render_operation = 'info'; status = 'online'
           available_workflows = $new; workflow_overrides = $me.workflow_overrides } | ConvertTo-Json -Depth 6 -Compress
if ($WhatIfOnly) { "WOULD POST: $body"; exit 0 }
try {
    $reply = Invoke-RestMethod -Uri $api -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 30
    Log ("$NodeName posted " + ($new -join ', ') + ' -> ok=' + $reply.ok)
} catch { Log ("$NodeName post failed: " + $_.Exception.Message); exit 1 }
