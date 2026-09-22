# Farm-box workflow advertiser (f5 / f15).
# Renderfin only dispatches a workflow to a box whose registry entry lists that
# file name, and these boxes run no agent of their own: the list was set by hand
# once and then silently outlived the models behind it. This does for them what
# deploy/onlyrender/worker-4090.ps1 does for the 4090 - derive the list from the
# files actually on disk - and posts it to the public renderfin API, which is
# the only route these boxes have to the VPS.
# Additive by design: a token this script does not know about is preserved, so
# a list another operator set is never clobbered.
param([string]$NodeName = $env:COMPUTERNAME, [switch]$WhatIfOnly)
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$models = 'D:\ComfyUI_windows_portable\ComfyUI\models'
$api = 'https://autorig.online/renderfin/api-render'
$log = 'C:\ProgramData\AutoRig\fleet-ltx23\advertise.log'
$blockFile = 'C:\ProgramData\AutoRig\fleet-ltx23\advertise_block.txt'
if ($NodeName -match '^F5') { $NodeName = 'f5' } elseif ($NodeName -match '^F15') { $NodeName = 'f15' }
function Log($m) { Add-Content -Path $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) -Encoding utf8 }
function Has([string]$rel) { Test-Path (Join-Path $models $rel) }

$tokens = @()
# LTXV 13B 0.9.8 distilled - the tier that already fans out.
if ((Has 'checkpoints\ltxv-13b-0.9.8-distilled-fp8.safetensors') -and (Has 'clip\t5xxl_fp8_e4m3fn.safetensors')) { $tokens += 'gen_animation_by_url.json' }
# LTX-2 19B distilled: the checkpoint carries its own text encoder and audio
# VAE; the template hard-codes the static camera LoRA and the Gemma encoder.
if ((Has 'checkpoints\ltx-2-19b-distilled-fp8.safetensors') -and
    (Has 'loras\ltx-2-19b-lora-camera-control-static.safetensors') -and
    (Has 'text_encoders\gemma_3_12B_it_fp4_mixed.safetensors')) { $tokens += 'gen_animation_hq_by_url.json' }
# LTX 2.3 distilled 1.1 is transformer-only, so every companion file must be here.
if ((Has 'diffusion_models\ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors') -and
    (Has 'text_encoders\ltx-2.3_text_projection_bf16.safetensors') -and
    (Has 'text_encoders\gemma_3_12B_it_fp4_mixed.safetensors') -and
    (Has 'vae\LTX23_video_vae_bf16.safetensors') -and
    (Has 'vae\LTX23_audio_vae_bf16.safetensors')) { $tokens += 'gen_animation_ltx23_by_url.json' }
# 10Eros is a full checkpoint on the same 2.3 base.
if ((Has 'checkpoints\ltx10eros_v14_2989669.safetensors') -and
    (Has 'text_encoders\gemma_3_12B_it_fp4_mixed.safetensors')) { $tokens += 'gen_animation_ltx10eros_by_url.json' }
# FLUX.2 klein image pair.
if ((Has 'diffusion_models\flux-2-klein-4b.safetensors') -and
    (Has 'text_encoders\qwen_3_4b_fp4_flux2.safetensors') -and
    (Has 'vae\flux2-vae.safetensors')) { $tokens += @('gen_image_flux2_klein.json','gen_image_flux2_klein_edit.json') }

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
