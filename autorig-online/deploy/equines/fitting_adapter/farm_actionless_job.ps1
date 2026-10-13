param(
    [Parameter(Mandatory=$true)][string]$CodeZip,
    [Parameter(Mandatory=$true)][string]$CodeZipSha256,
    [Parameter(Mandatory=$true)][string]$Blender,
    [Parameter(Mandatory=$true)][string]$PythonExe,
    [Parameter(Mandatory=$true)][string]$SourceGlb,
    [Parameter(Mandatory=$true)][string]$RigJson,
    [Parameter(Mandatory=$true)][string]$TaskId,
    [Parameter(Mandatory=$true)][string]$RunId,
    [Parameter(Mandatory=$true)][string]$WorkRoot
)
$ErrorActionPreference = 'Stop'
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $CodeZip).Hash.ToLower() -ne $CodeZipSha256.ToLower()) {
    throw 'code zip SHA-256 mismatch'
}
foreach ($path in @($Blender,$PythonExe,$SourceGlb,$RigJson)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "missing input: $path" }
}
if (Test-Path -LiteralPath $WorkRoot) { throw "work root already exists: $WorkRoot" }
New-Item -ItemType Directory -Path $WorkRoot | Out-Null
$code = Join-Path $WorkRoot 'code'
$bundle = Join-Path $WorkRoot 'actionless-bundle'
$anchors = Join-Path $WorkRoot 'tracker-anchor-map.json'
$adapter = Join-Path $WorkRoot 'adapter.json'
Expand-Archive -LiteralPath $CodeZip -DestinationPath $code
& $PythonExe (Join-Path $code 'fitting_adapter\prepare_fitting_adapter.py') prepare `
  --source $SourceGlb --rig-json $RigJson --species zebra --task-id $TaskId --run-id $RunId --output $adapter
if ($LASTEXITCODE -ne 0) { throw "adapter preparation failed: $LASTEXITCODE" }
& $Blender --background --factory-startup --python (Join-Path $code 'animation_fitting\render_actionless_bundle.py') -- `
  --input $SourceGlb --output-dir $bundle --species zebra --rig-type V3_EQUINE --camera-preset side
if ($LASTEXITCODE -ne 0) { throw "Blender actionless exporter failed: $LASTEXITCODE" }
$manifest = Join-Path $bundle 'immutable_manifest.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) { throw 'exporter produced no immutable manifest' }
$size = (Get-ChildItem -LiteralPath $bundle -File | Measure-Object Length -Sum).Sum
if ($size -gt 64MB) { throw "actionless bundle exceeds 64 MiB: $size" }
& $PythonExe (Join-Path $code 'fitting_adapter\prepare_fitting_adapter.py') bind-anchors `
  --adapter $adapter --bundle $bundle --output $anchors
if ($LASTEXITCODE -ne 0) { throw "anchor binding failed: $LASTEXITCODE" }
$result = [ordered]@{
  schema='autorig.equine-actionless-farm-result/1'; source_glb=(Get-FileHash -Algorithm SHA256 -LiteralPath $SourceGlb).Hash.ToLower();
  bundle_manifest=(Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash.ToLower();
  tracker_anchor_map=(Get-FileHash -Algorithm SHA256 -LiteralPath $anchors).Hash.ToLower(); bytes=[int64]$size;
  blender=(Get-Item -LiteralPath $Blender).VersionInfo.FileVersion
}
$result | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $WorkRoot 'result.json') -Encoding utf8
$result | ConvertTo-Json
