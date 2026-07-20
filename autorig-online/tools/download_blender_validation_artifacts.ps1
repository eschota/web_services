param(
    [string]$OutputDirectory = "$env:TEMP\autorig-blender-validation-20260720",
    [string]$LocalWorkerRoot = 'C:\NDLWebServerBuild\wwwroot\converter\glb'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

$files = [ordered]@{
    'human_all_animations.fbx' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_all_animations_unity.fbx'
    'human_all_animations.glb' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_all_animations.glb'
    'human_all_animations.blend' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_all_animations.blend'
    'human_prepared.glb' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_model_prepared.glb'
    'human_rigged.blend' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_model_prepared_rigged.blend'
    'human_custom_chicken_dance.fbx' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_Chicken_Dance.fbx'
    'human_hdrp.unitypackage' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_hdrp.unitypackage'
    'human_video.mp4' = 'https://converter-f1.freestock.online/converter/glb/aa2890c2-a957-4818-9753-cfeb50e55327/aa2890c2-a957-4818-9753-cfeb50e55327_video_small.mp4'
    'animal_rabbit_pack.fbx' = 'https://converter-f1.freestock.online/converter/glb/a02612e8-5b16-4860-bbb9-02b807bb1811/a02612e8-5b16-4860-bbb9-02b807bb1811_all_animations_unity.fbx'
    'animal_rabbit_animations.glb' = 'https://converter-f1.freestock.online/converter/glb/a02612e8-5b16-4860-bbb9-02b807bb1811/a02612e8-5b16-4860-bbb9-02b807bb1811_all_animations.glb'
    'animal_rabbit_rigged.blend' = 'https://converter-f1.freestock.online/converter/glb/a02612e8-5b16-4860-bbb9-02b807bb1811/a02612e8-5b16-4860-bbb9-02b807bb1811_model_prepared_rigged.blend'
}

foreach ($entry in $files.GetEnumerator()) {
    $target = Join-Path $OutputDirectory $entry.Key
    if (-not (Test-Path $target) -or (Get-Item $target).Length -eq 0) {
        $uri = [Uri]$entry.Value
        $relativePath = [Uri]::UnescapeDataString(($uri.AbsolutePath -replace '^/converter/glb/', ''))
        $localSource = Join-Path $LocalWorkerRoot ($relativePath -replace '/', '\')
        if (Test-Path -LiteralPath $localSource) {
            Copy-Item -LiteralPath $localSource -Destination $target
        }
        else {
            Invoke-WebRequest -UseBasicParsing -Uri $entry.Value -OutFile $target -TimeoutSec 300
        }
    }
    Get-Item $target | Select-Object Name, Length
}

$customZip = Join-Path $OutputDirectory 'human_custom_with_base.zip'
Compress-Archive -Path @(
    (Join-Path $OutputDirectory 'human_all_animations.fbx'),
    (Join-Path $OutputDirectory 'human_custom_chicken_dance.fbx')
) -DestinationPath $customZip -Force

$bundleZip = Join-Path $OutputDirectory 'human_full_bundle.zip'
Compress-Archive -Path @(
    (Join-Path $OutputDirectory 'human_all_animations.fbx'),
    (Join-Path $OutputDirectory 'human_all_animations.glb'),
    (Join-Path $OutputDirectory 'human_all_animations.blend'),
    (Join-Path $OutputDirectory 'human_prepared.glb'),
    (Join-Path $OutputDirectory 'human_rigged.blend'),
    (Join-Path $OutputDirectory 'human_hdrp.unitypackage'),
    (Join-Path $OutputDirectory 'human_video.mp4')
) -DestinationPath $bundleZip -Force

Get-Item $customZip, $bundleZip | Select-Object Name, Length
