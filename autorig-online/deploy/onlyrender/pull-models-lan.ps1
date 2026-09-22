# Pull the LTX-2.5 set from a farm peer over the LAN (peer serves its models dir on :18998).
param([string]$Peer = 'http://192.168.0.108:18998')
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$models = 'D:\ComfyUI_windows_portable\ComfyUI\models'
$log = 'C:\ProgramData\AutoRig\ltx25\lan.log'
$peer = $Peer
function Log($m) { Add-Content -Path $log -Value ((Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m) }
$items = @(
  @{ rel='vae/ltx-2.5-audio-vae-bf16.safetensors'; s=364866540; h='c52733d37f6a7fb7949c3dc0fb468c6cb2169e4d836983a73babb9f0d54837a5' },
  @{ rel='vae/ltx-2.5-video-vae-bf16.safetensors'; s=1472223346; h='847e14ca7f3355debca0cea4eaa24ac0fbcdf0061da054ac89ca638a869ddba3' },
  @{ rel='text_encoders/gemma4-12b-with-proj-ltx-2.5-comfy-int8-convrot.safetensors'; s=15372971786; h='09a89e084de1a149c3de60cfe9dfd3e5161967eb09eea39e806fcdeffdd568de' },
  @{ rel='diffusion_models/ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors'; s=21504034224; h='c4279eeff115cbeaca494bd2183e7d768c38fe85a184dc6afbb7159157c44334' },
  @{ rel='latent_upscale_models/ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors'; s=995778752; h='eb5a71fe4068ee87ccdb1c3aa635e547ca76bd2d30ae20ae889f2c325c0677e8' }
)
function PeerSize($rel) {
  $out = & curl.exe -s -I --connect-timeout 5 --max-time 20 "$peer/$rel" 2>$null
  foreach ($line in $out) { if ($line -match '(?i)^Content-Length:\s*(\d+)') { return [int64]$Matches[1] } }
  return 0
}
Log "=== LAN puller start ==="
$deadline = (Get-Date).AddHours(8)
foreach ($it in $items) {
  $final = Join-Path $models ($it.rel -replace '/', '\')
  $part = $final + '.part'
  $dir = Split-Path $final
  if ((Test-Path $final) -and ((Get-Item $final).Length -eq $it.s)) { Log "SKIP $($it.rel) present"; continue }
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $done = $false
  while (-not $done -and (Get-Date) -lt $deadline) {
    $ps = PeerSize $it.rel
    if ($ps -ne $it.s) { Log "WAIT $($it.rel) peer has $ps / $($it.s)"; Start-Sleep -Seconds 60; continue }
    Remove-Item $part -Force -ErrorAction SilentlyContinue
    $sw = [Diagnostics.Stopwatch]::StartNew()
    & curl.exe -f -s --connect-timeout 10 --speed-time 60 --speed-limit 1048576 -o "$part" "$peer/$($it.rel)" 2>$null
    $rc = $LASTEXITCODE; $sw.Stop()
    $have = if (Test-Path $part) { (Get-Item $part).Length } else { 0 }
    Log ("GOT $($it.rel) rc=$rc have=$have secs=" + [math]::Round($sw.Elapsed.TotalSeconds, 1))
    if ($have -ne $it.s) { Start-Sleep -Seconds 20; continue }
    $sha = (Get-FileHash -Path $part -Algorithm SHA256).Hash.ToLower()
    if ($sha -ne $it.h) { Log "FAIL sha $sha"; Remove-Item $part -Force; Start-Sleep -Seconds 20; continue }
    Move-Item -Force $part $final
    Log "OK $final"
    $done = $true
  }
}
Log "=== LAN puller end ==="
