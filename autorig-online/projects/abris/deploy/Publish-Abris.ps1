param(
    [string]$SourceRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$StagingRoot = (Join-Path (Split-Path -Parent $PSScriptRoot) '.work/deploy-stage'),
    [string]$SshHost = 'autorig-vps'
)

$ErrorActionPreference = 'Stop'
$SourceRoot = (Resolve-Path -LiteralPath $SourceRoot).Path
New-Item -ItemType Directory -Force -Path $StagingRoot | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$archive = Join-Path $StagingRoot "abris-$stamp.tar.gz"
$python = Get-Command python -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $python) { $python = Get-Command py -CommandType Application | Select-Object -First 1 }

# Publish source only. Python caches and private test uploads remain local.
@'
from pathlib import Path
import sys
import tarfile

source = Path(sys.argv[1])
archive = Path(sys.argv[2])
suffixes = {
    "backend": {".py", ".txt"},
    "static": {".html", ".css", ".js", ".mjs", ".json", ".png", ".jpg", ".jpeg", ".svg", ".webp", ".woff2"},
    "deploy": {".conf", ".ps1", ".service", ".timer", ".sh", ".md"},
}
with tarfile.open(archive, "x:gz") as package:
    for section in suffixes:
        for file in sorted((source / section).rglob("*")):
            rel = file.relative_to(source)
            if (
                file.is_file()
                and not file.is_symlink()
                and file.suffix in suffixes[section]
                and not any(part.startswith(".") or part in {"__pycache__", "qa", "tests"} for part in rel.parts)
                and not file.name.startswith("test_")
            ):
                package.add(file, rel.as_posix(), recursive=False)
'@ | & $python.Source -X utf8 - $SourceRoot $archive
if ($LASTEXITCODE -ne 0) { throw 'Abris source packaging failed' }

$checksum = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
$release = "$($checksum.Substring(0, 12))-$stamp"
$remoteIncoming = '/srv/autorig/abris/incoming'
& ssh -o BatchMode=yes $SshHost "sudo -n install -d -m 0755 /srv/autorig/abris && sudo -n install -d -o debian -g debian -m 0700 $remoteIncoming"
if ($LASTEXITCODE -ne 0) { throw 'Abris release directory unavailable' }
& scp -q $archive "${SshHost}:$remoteIncoming/$release.tar.gz"
if ($LASTEXITCODE -ne 0) { throw 'Abris release upload failed' }
& scp -q (Join-Path $SourceRoot 'deploy/deploy.sh') "${SshHost}:$remoteIncoming/deploy-$release.sh"
if ($LASTEXITCODE -ne 0) { throw 'Abris deployment script upload failed' }
& ssh -o BatchMode=yes $SshHost "sudo -n bash $remoteIncoming/deploy-$release.sh $release $checksum"
if ($LASTEXITCODE -ne 0) { throw 'Abris deployment failed' }
