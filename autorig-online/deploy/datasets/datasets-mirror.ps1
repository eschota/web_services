# AutoRig datasets mirror (owner 2026-10-16: «надежное хранилище для датасетов, которое не будет постоянно стираться»).
# The second copy of https://autorig.online/datasets/files/ on Raptor: every hour the scheduled task
# «AutoRig Datasets Mirror» (SYSTEM) pulls every set listed by /datasets/api/list into X:\datasets\<set>\, the picture and
# its sidecar <name>.json. A file is downloaded once, checked against the sha256 the store lists, then renamed into place.
# Never deletes anything. Token: C:\ProgramData\AutoRig\datasets-mirror\token.txt (DATASETS_TOKEN; not in Git).
# Log: C:\ProgramData\AutoRig\datasets-mirror\mirror.log; last summary: status.json.
#   powershell -ExecutionPolicy Bypass -File datasets-mirror.ps1 [-Verify]   (-Verify re-hashes every local file)
param([switch]$Verify, [string]$Dest = "X:\datasets", [string]$Base = "https://autorig.online")
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$Home_ = "C:\ProgramData\AutoRig\datasets-mirror"
$Log = Join-Path $Home_ "mirror.log"
function Log($m) { $line = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") + " " + $m; Add-Content -Path $Log -Value $line -Encoding UTF8; Write-Output $line }
$lock = Join-Path $Home_ "mirror.lock"
try { $lk = [IO.File]::Open($lock, 'OpenOrCreate', 'ReadWrite', 'None') } catch { Log "another run holds the lock"; exit 0 }
try {
    $token = (Get-Content (Join-Path $Home_ "token.txt") -Raw).Trim()
    $h = @{ Authorization = "Bearer $token" }
    $wc = New-Object Net.WebClient
    $wc.Headers.Add("User-Agent", "autorig-datasets-mirror/1")
    $sets = (Invoke-RestMethod -Uri "$Base/datasets/api/sets" -Headers $h -TimeoutSec 120).sets
    $tot = 0; $new = 0; $bad = 0; $bytes = 0; $checked = 0
    foreach ($set in $sets) {
        $list = Invoke-RestMethod -Uri ("$Base/datasets/api/list?set=" + [Uri]::EscapeDataString($set)) -Headers $h -TimeoutSec 300
        $dir = Join-Path $Dest ($set -replace '/', '\')
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        foreach ($f in $list.files) {
            $tot++
            $target = Join-Path $dir $f.name
            $have = Test-Path -LiteralPath $target
            if ($have -and ((Get-Item -LiteralPath $target).Length -eq [int64]$f.bytes)) {
                if ($Verify) {
                    $checked++
                    if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLower() -ne $f.sha256) {
                        Log "SHA MISMATCH $set/$($f.name)"; $bad++
                    }
                }
            } else {
                $tmp = "$target.part"
                $wc.DownloadFile($f.url, $tmp)
                $sha = (Get-FileHash -LiteralPath $tmp -Algorithm SHA256).Hash.ToLower()
                if ($sha -ne $f.sha256) { Log "bad download $set/$($f.name) sha $sha"; Remove-Item -LiteralPath $tmp -Force; $bad++; continue }
                if ($have) { Rename-Item -LiteralPath $target -NewName ($f.name + ".old-" + (Get-Date -Format yyyyMMddHHmmss)) }
                Move-Item -LiteralPath $tmp -Destination $target
                $new++; $bytes += [int64]$f.bytes
            }
            $side = "$target.json"
            if (-not (Test-Path -LiteralPath $side)) {
                $wc.DownloadFile($f.sidecar_url, "$side.part"); Move-Item -LiteralPath "$side.part" -Destination $side
            }
        }
    }
    $local = (Get-ChildItem -LiteralPath $Dest -Recurse -File | Where-Object { $_.Name -notlike "*.json" -and $_.Name -notlike "*.part" -and $_.Name -notlike "*.old-*" }).Count
    $status = [ordered]@{ time = (Get-Date).ToUniversalTime().ToString("o"); sets = $sets.Count; listed = $tot; local_files = $local;
                          downloaded = $new; mb = [math]::Round($bytes / 1MB, 1); bad = $bad; verified = $checked; match = ($local -ge $tot -and $bad -eq 0) }
    ($status | ConvertTo-Json) | Set-Content -Path (Join-Path $Home_ "status.json") -Encoding UTF8
    Log ("sets {0} listed {1} local {2} new {3} ({4} MB) bad {5} verified {6}" -f $sets.Count, $tot, $local, $new, $status.mb, $bad, $checked)
} catch {
    Log ("ERROR " + $_.Exception.Message)
    exit 1
} finally { $lk.Close() }
