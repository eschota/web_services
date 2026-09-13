[CmdletBinding()]
param([string]$OutputDirectory = (Join-Path (Split-Path $PSScriptRoot -Parent) '.work\route-qa'))
$ErrorActionPreference='Stop'
$outputRoot=[IO.Path]::GetFullPath($OutputDirectory)
$serviceRoot=[IO.Path]::GetFullPath((Split-Path $PSScriptRoot -Parent))
if (-not $outputRoot.StartsWith($serviceRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'QA output must be inside the SandFlow service project.' }
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
$files=@('index.html','TemplateData/style.css','Build/web.loader.js','Build/web.framework.js.unityweb','Build/web.wasm.unityweb','Build/web.data.unityweb')
$results=[Collections.Generic.List[object]]::new()
foreach($relative in $files){
    $url='https://autorig.online/sandflow/' + $relative
    $file=Join-Path $outputRoot $relative
    New-Item -ItemType Directory -Force -Path (Split-Path $file -Parent) | Out-Null
    $headers=$file + '.headers'
    $status=& curl.exe --silent --show-error --fail --max-time 90 --dump-header $headers --output $file --write-out '%{http_code}' $url
    if($LASTEXITCODE -ne 0 -or $status -ne '200'){throw "Resource failed: $relative ($status)"}
    $headerText=Get-Content -LiteralPath $headers -Raw
    if($relative.EndsWith('.unityweb') -and $headerText -match '(?im)^Content-Encoding:'){throw "Unity fallback payload must be served verbatim: $relative"}
    if($relative -eq 'index.html' -and (Get-Content -LiteralPath $file -Raw) -notmatch 'unity-canvas'){throw 'Not a Unity client page'}
    $results.Add([ordered]@{path=$relative; status=200; bytes=(Get-Item -LiteralPath $file).Length; sha256=(Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash})
    Write-Output "PASS $relative"
}
$redirectHeaders=Join-Path $outputRoot 'redirect.headers'
$redirect=& curl.exe --silent --show-error --max-time 20 --output NUL --dump-header $redirectHeaders --write-out '%{http_code}' 'https://autorig.online/sandflow'
if($LASTEXITCODE -ne 0 -or $redirect -ne '301' -or (Get-Content -LiteralPath $redirectHeaders -Raw) -notmatch '(?im)^Location: https://autorig\.online/sandflow/\s*$'){throw 'Canonical redirect failed'}
$missing=& curl.exe --silent --show-error --max-time 20 --output NUL --write-out '%{http_code}' 'https://autorig.online/sandflow/__qa_missing__.wasm'
if($missing -ne '404'){throw 'Missing resource must not return a fake HTML success'}
$rootPage=Join-Path $outputRoot 'root.html'
$rootCode=& curl.exe --silent --show-error --fail --location --max-time 20 --output $rootPage --write-out '%{http_code}' 'https://autorig.online/sandflow'
if($rootCode -ne '200' -or (Get-FileHash -LiteralPath $rootPage).Hash -ne (Get-FileHash -LiteralPath (Join-Path $outputRoot 'index.html')).Hash){throw 'Canonical root does not serve the exact Unity page'}
$hidden=& curl.exe --silent --show-error --max-time 20 --output NUL --write-out '%{http_code}' 'https://autorig.online/sandflow/.git/config'
if($hidden -ne '403'){throw 'Existing server-wide hidden-file deny must remain active'}
$health=Invoke-RestMethod -Uri 'https://autorig.online/sandflow/health'
if($health.service -ne 'sandflow' -or $health.admissionEnabled){throw 'Backend admission gate unexpectedly changed'}
$regressions=[ordered]@{}
foreach($url in @('https://autorig.online/realflow/','https://autorig.online/gallery','https://map.autorig.online/')){
    $code=& curl.exe --silent --show-error --max-time 20 --output NUL --write-out '%{http_code}' $url
    if($LASTEXITCODE -ne 0 -or $code -ne '200'){throw "Regression: $url ($code)"}
    $regressions[$url]=[int]$code
}
$report=[ordered]@{schemaVersion=1; checkedUtc=[DateTime]::UtcNow.ToString('o'); status='PASS'; scope='HTTP routing and exact resource delivery, NOT browser execution or multiplayer'; clientRelease='sandflow-v0135-f957395'; backendRevision=$health.revision; admissionEnabled=$health.admissionEnabled; redirect=301; root=200; missingResource=404; hiddenResource=403; assets=$results; regressions=$regressions}
# Generated verification artifact; no source or user data is rewritten.
$report | ConvertTo-Json -Depth 7 | Set-Content -LiteralPath (Join-Path $outputRoot 'report.json') -Encoding utf8
Write-Output 'PASS redirect, missing-resource behavior, gated backend and unaffected realflow/gallery/map routes.'
