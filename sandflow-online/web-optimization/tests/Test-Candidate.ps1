[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$root=[IO.Path]::GetFullPath((Split-Path $PSScriptRoot -Parent))
$html=Get-Content -LiteralPath (Join-Path $root 'template\index.html') -Raw
$qaHtml=Get-Content -LiteralPath (Join-Path $root 'template-qa\index.html') -Raw
$js=Get-Content -LiteralPath (Join-Path $root 'template\TemplateData\sandflow-bootstrap.js') -Raw
$nginx=Get-Content -LiteralPath (Join-Path $root 'delivery-rules.nginx.conf') -Raw
$qaNginx=Get-Content -LiteralPath (Join-Path $root 'delivery-rules-qa.nginx.conf') -Raw
$buildContract=Get-Content -LiteralPath (Join-Path $root 'webqa-build-contract.json') -Raw | ConvertFrom-Json

foreach($macro in @('LOADER_FILENAME','DATA_FILENAME','FRAMEWORK_FILENAME','CODE_FILENAME','PRODUCT_NAME','PRODUCT_VERSION')){
    if($html -notmatch [regex]::Escape('{{{ ' + $macro)){throw "Missing Unity macro: $macro"}
    if($qaHtml -notmatch [regex]::Escape('{{{ ' + $macro)){throw "Missing QA Unity macro: $macro"}
}
if($html -notmatch '<base href="/sandflow/">'){throw 'Deep links would resolve build assets under /sandflow/s/.'}
if($html -notmatch 'mode: "preview"' -or $html -notmatch 'surface: "public-preview"'){
    throw 'Normal template must remain the explicit preview surface.'
}
if(($qaHtml -notmatch '<base href="/sandflow/qa/">') -or ($qaHtml -notmatch 'mode: "live"') -or ($qaHtml -notmatch 'Online QA / not public release')){
    throw 'QA template base, live mode or visible non-public label is missing.'
}
if($qaHtml -notmatch 'id="mode-badge" class="qa-badge"'){
    throw 'QA badge must use its non-overlapping QA-only layout class.'
}
if($html -match '(?i)password|bearer|steamid'){throw 'Sensitive credential concept leaked into HTML template.'}
if($qaHtml -match '(?i)password|bearer|steamid'){throw 'Sensitive credential concept leaked into QA HTML template.'}
foreach($token in @('navigator.gpu','requestAdapter','isSecureContext','WebAssembly','WebSocket','stallWarningSeconds','capabilityTimeoutSeconds','loadTimeoutSeconds','createLoadState','acceptUnity')){
    if($js -notmatch [regex]::Escape($token)){throw "Missing capability/loading guard: $token"}
}
if($nginx -match '(?im)^\s*add_header\s+Content-Encoding'){throw 'Fallback .unityweb must not receive Content-Encoding.'}
if($nginx -notmatch 'default_type application/octet-stream' -or $nginx -notmatch '__SANDFLOW_RELEASE_ROOT__'){
    throw 'Fallback container type or explicit release-root integration gate is missing.'
}
if($qaNginx -notmatch '__SANDFLOW_QA_RELEASE_ROOT__' -or $qaNginx -notmatch 'X-Robots-Tag "noindex, nofollow, noarchive"'){
    throw 'QA release-root or search-index exclusion is missing.'
}
if(([regex]::Matches($qaNginx,'add_header Cache-Control "no-store" always;').Count -ne 3) -or ($qaNginx -match '(?im)^\s*add_header\s+Cache-Control[^\r\n]*(?:immutable|max-age)') -or ($qaNginx -match '(?im)^\s*add_header\s+Content-Encoding')){
    throw 'Every QA HTML/asset response must remain no-store and fallback payloads unencoded.'
}
if(-not $qaNginx.Contains('location ~ "^/sandflow/qa/s/[0-9a-fA-F]{32}$" {')){
    throw 'Nginx regex quantifier braces require a quoted location expression.'
}
if(($qaNginx -notmatch '(?s)location = /sandflow/qa/ \{\s*alias __SANDFLOW_QA_RELEASE_ROOT__/;\s*index index\.html;') -or ($qaNginx -match '(?s)location = /sandflow/qa/ \{\s*alias __SANDFLOW_QA_RELEASE_ROOT__/index\.html;')){
    throw 'Exact QA root must use a directory alias plus index, never a file alias.'
}
if(($qaNginx -match '(?m)^location\s+(?:\^~\s+)?/sandflow/(?:\s|\{)') -or ($qaNginx -match '(?m)^location[^\r\n]*/sandflow/(?:api|ws|health)')){
    throw 'QA static layout must not redefine public root, API, WS or health routes.'
}
$css=Get-Content -LiteralPath (Join-Path $root 'template\TemplateData\style.css') -Raw
if(($css -notmatch '#mode-badge\.qa-badge[^\{]*\{[^\}]*right:\s*max\(84px,') -or ($css -notmatch '#mode-badge\.qa-badge[^\{]*\{[^\}]*pointer-events:\s*none')){
    throw 'QA badge must reserve 84px at top-right and not intercept input.'
}
if(($buildContract.schemaVersion -ne 1) -or ($buildContract.profile -ne 'WebQa') -or ($buildContract.template -ne 'PROJECT:SandFlowOnlineQa') -or (-not $buildContract.nameFilesAsHashes) -or (-not $buildContract.dataCaching) -or ($buildContract.routeBase -ne '/sandflow/qa/') -or ($buildContract.cachePolicy -ne 'no-store')){
    throw 'WebQA hashed-file/cache integration contract is invalid.'
}

$node=Get-Command node -ErrorAction Stop
& $node.Source (Join-Path $PSScriptRoot 'bootstrap.test.js')
if($LASTEXITCODE -ne 0){throw "Node tests failed: $LASTEXITCODE"}
Write-Output 'PASS template macros, capability guards, fallback delivery and deterministic JS tests.'
