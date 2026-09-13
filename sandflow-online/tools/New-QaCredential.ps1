$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$folder=Join-Path $root '.work\qa-session'
New-Item -ItemType Directory -Force -Path $folder | Out-Null
$target=Join-Path $folder 'server.env'
if(Test-Path -LiteralPath $target){throw 'QA credential already exists; reuse it, do not rotate silently.'}
$bytes=[byte[]]::new(32)
$rng=[Security.Cryptography.RandomNumberGenerator]::Create()
try{$rng.GetBytes($bytes)}finally{$rng.Dispose()}
$secret=([BitConverter]::ToString($bytes)).Replace('-','').ToLowerInvariant()
# Generated service credential, not source code. It is ignored and must never be printed or committed.
[IO.File]::WriteAllText($target,"SANDFLOW_QA_TOKEN=$secret`n",[Text.UTF8Encoding]::new($false))
Write-Output 'Created project-local ignored QA credential; value not displayed.'
