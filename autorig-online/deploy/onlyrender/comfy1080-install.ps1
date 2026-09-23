# Isolated ComfyUI 0.37.0 image worker for the GTX 1080 Ti converter boxes
# (f1, f2, f7, f11, f13). Pascal (sm_61) is gone from the current CUDA 12.8+
# PyTorch wheels, but the Hunyuan runtime already on these boxes carries
# torch 2.8.0+cu126, which still ships sm_61. Its interpreter is COPIED (never
# modified) into a private root, so nothing the converter runs is touched.
# Models come from a farm LAN peer (:18998) and are sha256-checked.
#
# Safe to re-run: every step skips work that is already done.
# Never uses robocopy /MIR (see memory robocopy-mir-xd-wipe).
param(
    [string]$Root = 'C:\AI\ComfyUI1080',
    [string]$SourcePython = '',
    [string]$Peer = 'http://192.168.0.115:18998',
    [string]$Sets = 'zimage,klein',
    [switch]$SkipModels
)
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$state = 'C:\ProgramData\AutoRig\comfy1080'
New-Item -ItemType Directory -Force -Path $state | Out-Null
$log = Join-Path $state 'install.log'
function Log($m) { $line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m; Add-Content -Path $log -Value $line; $line }

if (-not $SourcePython) {
    $SourcePython = @('C:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone',
                      'D:\AI\HY3D2\Hunyuan3D2_WinPortable\python_standalone') |
        Where-Object { Test-Path (Join-Path $_ 'python.exe') } | Select-Object -First 1
}
if (-not $SourcePython) { Log 'FAIL no Hunyuan python_standalone found'; exit 2 }
$git = Join-Path (Split-Path $SourcePython) 'MinGit\cmd\git.exe'
$py = Join-Path $Root 'python\python.exe'
$comfy = Join-Path $Root 'ComfyUI'
Log "=== install start root=$Root source=$SourcePython"

# 1. Private interpreter (copy, not link).
if (-not (Test-Path $py)) {
    New-Item -ItemType Directory -Force -Path (Join-Path $Root 'python') | Out-Null
    & robocopy.exe $SourcePython (Join-Path $Root 'python') /E /R:1 /W:1 /NFL /NDL /NJH /NP /XD __pycache__ | Out-Null
    Log "python copied rc=$LASTEXITCODE"
}
$probe = & $py -c "import torch,sys;print(torch.__version__, torch.cuda.is_available(), 'sm_61' in torch.cuda.get_arch_list())" 2>&1
Log "torch probe: $probe"

# 2./3. ComfyUI v0.37.0 source and its requirements. The farm WAN is shared
# and can crawl, so a peer's transfer folder (git archive of v0.37.0 plus its
# installed distributions, see comfy1080_lan_deps.py) is used when present;
# git + pip are the fallback.
$transfer = "$Peer/_comfy1080_transfer"
$lan = $false
& curl.exe -s -f -o NUL --connect-timeout 5 "$transfer/index.json"
if ($LASTEXITCODE -eq 0) { $lan = $true }
if (-not (Test-Path (Join-Path $comfy 'main.py'))) {
    if ($lan) {
        $zip = Join-Path $state 'comfyui-v0.37.0.zip'
        & curl.exe -s -f -o $zip "$transfer/comfyui-v0.37.0.zip"
        if (Test-Path $comfy) { Remove-Item -Recurse -Force $comfy }
        Expand-Archive -Path $zip -DestinationPath $comfy -Force
        Log "comfy source from LAN rc=$LASTEXITCODE"
    } else {
        & $git clone --depth 1 --branch v0.37.0 https://github.com/comfyanonymous/ComfyUI.git $comfy 2>&1 | Out-Null
        Log "comfy clone rc=$LASTEXITCODE"
    }
}
$ver = Get-Content (Join-Path $comfy 'comfyui_version.py') -ErrorAction SilentlyContinue | Select-String '__version__'
Log "comfy version: $ver"
if ($lan) {
    # The workflow-template media packages (~500 MB) only feed the browser UI.
    $roots = @('comfyui-frontend-package','comfyui-workflow-templates-core','comfyui-workflow-templates-json',
               'comfyui-embedded-docs','torchsde','alembic','SQLAlchemy','av','comfy-kitchen','comfy-aimdo',
               'simpleeval','blake3','spandrel','pydantic-settings','PyOpenGL','comfy-angle')
    $out = & $py (Join-Path $state 'comfy1080_lan_deps.py') $transfer @roots 2>&1
    Log "lan deps: $out"
} else {
    $req = Join-Path $state 'requirements.notorch.txt'
    Get-Content (Join-Path $comfy 'requirements.txt') | Where-Object { $_ -notmatch '^\s*torch(vision|audio)?\s*([<>=!~].*)?$' } | Set-Content -Path $req -Encoding ascii
    & $py -m pip install --disable-pip-version-check --no-warn-script-location -r $req *>> (Join-Path $state 'pip.log')
    Log "pip requirements rc=$LASTEXITCODE"
}

# 4. Models from the LAN peer, sha256-checked.
$items = @(
  @{ set='zimage'; rel='diffusion_models/z_image_turbo_fp8_e4m3fn.safetensors'; s=6154958896; h='56c06ad6cb80941e8203d3e248a16ad1263499ba870f2c1931aa724fdc7c25d3' },
  @{ set='zimage,klein'; rel='text_encoders/qwen_3_4b.safetensors'; s=8044982048; h='6c671498573ac2f7a5501502ccce8d2b08ea6ca2f661c458e708f36b36edfc5a' },
  @{ set='zimage'; rel='vae/ae.safetensors'; s=335304388; h='afc8e28272cd15db3919bacdb6918ce9c1ed22e96cb12c4d5ed0fba823529e38' },
  @{ set='control'; rel='model_patches/Z-Image-Turbo-Fun-Controlnet-Union-2.1-2602-8steps.safetensors'; s=6712485600; h='d1251cc7bc3486bc61d25c3be498ef394c31c85ddf4ee9137d2e933411f4a689' },
  @{ set='klein'; rel='diffusion_models/flux-2-klein-4b.safetensors'; s=7751105712; h='ec3d4e733a771f61c052fb4856c48b336c55eaf2c65487c2a1faeb9bbda7a343' },
  @{ set='klein'; rel='text_encoders/qwen_3_4b_fp4_flux2.safetensors'; s=3848213998; h='3eab03a77adb0ee5304a4e677d5c10ac22f9049c1d7c894adca4f8bb39206ca8' },
  @{ set='klein'; rel='vae/flux2-vae.safetensors'; s=336211292; h='868fe7b343cc8f3a19dbcfcafbc3d5f888802be3f89bd81b65b3621a066ce8f3' }
)
$want = @($Sets.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
if (-not $SkipModels) {
    foreach ($it in $items) {
        $hit = @($it.set.Split(',') | Where-Object { $want -contains $_ })
        if ($hit.Count -eq 0) { continue }
        $final = Join-Path (Join-Path $comfy 'models') ($it.rel -replace '/', '\')
        $part = $final + '.part'
        New-Item -ItemType Directory -Force -Path (Split-Path $final) | Out-Null
        if ((Test-Path $final) -and ((Get-Item $final).Length -eq $it.s)) { Log "SKIP $($it.rel) present"; continue }
        $free = (Get-PSDrive ($Root.Substring(0,1))).Free
        if ($free -lt ($it.s + 8GB)) { Log "FAIL disk: $([math]::Round($free/1GB,1)) GB free, need $([math]::Round(($it.s+8GB)/1GB,1))"; exit 3 }
        for ($try = 1; $try -le 3; $try++) {
            Remove-Item $part -Force -ErrorAction SilentlyContinue
            $sw = [Diagnostics.Stopwatch]::StartNew()
            & curl.exe -f -s --connect-timeout 10 --speed-time 60 --speed-limit 1048576 -o "$part" "$Peer/$($it.rel)" 2>$null
            $rc = $LASTEXITCODE; $sw.Stop()
            $have = if (Test-Path $part) { (Get-Item $part).Length } else { 0 }
            Log ("GOT $($it.rel) rc=$rc have=$have secs=" + [math]::Round($sw.Elapsed.TotalSeconds, 1))
            if ($have -ne $it.s) { Start-Sleep -Seconds 15; continue }
            $sha = (Get-FileHash -Path $part -Algorithm SHA256).Hash.ToLower()
            if ($sha -ne $it.h) { Log "FAIL sha $($it.rel) $sha"; Remove-Item $part -Force; continue }
            Move-Item -Force $part $final
            Log "OK $($it.rel)"
            break
        }
    }
}
Log '=== install end'
