[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$root=[IO.Path]::GetFullPath((Split-Path $PSScriptRoot -Parent))
$html=Get-Content -LiteralPath (Join-Path $root 'template\index.html') -Raw
$js=Get-Content -LiteralPath (Join-Path $root 'template\TemplateData\sandflow-bootstrap.js') -Raw
$nginx=Get-Content -LiteralPath (Join-Path $root 'delivery-rules.nginx.conf') -Raw

foreach($macro in @('LOADER_FILENAME','DATA_FILENAME','FRAMEWORK_FILENAME','CODE_FILENAME','PRODUCT_NAME','PRODUCT_VERSION')){
    if($html -notmatch [regex]::Escape('{{{ ' + $macro)){throw "Missing Unity macro: $macro"}
}
if($html -notmatch '<base href="/sandflow/">'){throw 'Deep links would resolve build assets under /sandflow/s/.'}
if($html -match '(?i)password|bearer|steamid'){throw 'Sensitive credential concept leaked into HTML template.'}
foreach($token in @('navigator.gpu','requestAdapter','isSecureContext','WebAssembly','WebSocket','stallWarningSeconds','capabilityTimeoutSeconds','loadTimeoutSeconds','createLoadState','acceptUnity')){
    if($js -notmatch [regex]::Escape($token)){throw "Missing capability/loading guard: $token"}
}
if($nginx -match '(?im)^\s*add_header\s+Content-Encoding'){throw 'Fallback .unityweb must not receive Content-Encoding.'}
if($nginx -notmatch 'default_type application/octet-stream' -or $nginx -notmatch '__SANDFLOW_RELEASE_ROOT__'){
    throw 'Fallback container type or explicit release-root integration gate is missing.'
}

$node=Get-Command node -ErrorAction Stop
& $node.Source (Join-Path $PSScriptRoot 'bootstrap.test.js')
if($LASTEXITCODE -ne 0){throw "Node tests failed: $LASTEXITCODE"}
Write-Output 'PASS template macros, capability guards, fallback delivery and deterministic JS tests.'
