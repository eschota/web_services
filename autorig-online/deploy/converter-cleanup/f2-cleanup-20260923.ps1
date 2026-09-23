<#
  AutoRig converter farm disk cleanup - box f2 (F2-PC) - reviewed 2026-09-23.

  Deletes ONLY the categories and exact paths/patterns listed in the "box plan" section below.
  Inventory and rationale: autorig-online/docs/converter-cleanup-20260923.md

  Default is a DRY RUN: it lists what it would delete and the GB it would free.
  Add -Apply to actually delete. Add -IncludeRecycleBin to also empty the
  user's Recycle Bin on C: (owner decision, off by default).

  Safety rules built in:
  * refuses to run on any machine except F2-PC;
  * refuses to delete while the local converter reports processing, pending
    or queued tasks (waits up to -WaitIdleMinutes, rechecked per category);
  * never touches models, Program Files, Windows, ProgramData\AutoRig,
    converter code, Unity projects, or task folders; converter task storage
    is only touched for <guid>.zip bundles whose byte-identical copy
    (same size and SHA-256) is already stored in the autorig.online
    artifact cache;
  * skips any file that is open by another process, is a reparse point,
    or resolves outside its category root;
  * logs every removed path and the freed GB to
    C:\ProgramData\AutoRigCleanup\f2-cleanup-<timestamp>.log

  ASCII only on purpose (Windows PowerShell 5.1 over ssh).
#>
param(
  [switch]$Apply,
  [switch]$IncludeRecycleBin,
  [int]$WaitIdleMinutes = 45
)

$ErrorActionPreference = 'Stop'


$Box        = 'f2'
$ExpectHost = 'F2-PC'
$StatusUrl  = 'http://127.0.0.1:5279/api-converter-glb/server-status'
$UserRoot   = 'C:\Users\user'
$Temp       = 'C:\Users\user\AppData\Local\Temp'
$GlbRoot    = 'C:\NDLWebServerBuild\wwwroot\converter\glb'
$Now        = Get-Date

# ---------------------------------------------------------------- box plan
# Mirrored ZIP bundles: guid|bytes|sha256 of the copy in
# /srv/autorig/data/artifact-cache/<task>/ on autorig.online (checked 2026-09-23).
$MirroredZips = @(
  '06553411-e863-4bc9-aae4-49746c3ef5af|344889348|83CB26E461F33F60D885642E6CDBDB4711489B13096E4DABCA5B795458CF3F53',
  '107f4543-eed7-4a1d-864b-2a337174aa36|269072391|4DB417DD0B9486D52F43C43394A218FEC4B42D19D4BDC8CDD15275804F3150DC',
  '1723c7e4-b24b-4d89-9496-f2b1359a9e15|251688493|32601C85E8F38BEED6A60D0A306361B4C10C51B6DF12D7B1686613CD00F0C95C',
  '51aa892e-5bd9-4701-a09a-6c58493cf75f|306716239|6CBCF6CB7ECA8F729FE88EABF8BAB03356DFF3E9320F3021297CB79E4499BC65',
  '8f350fa5-cece-4ec9-b9a5-a76b3f6f4195|951957142|F51BF4AD89EFD2A2055CF4EE4B84A6435F27C6A4C7CF73177209D9D112036C4B',
  'a861a111-16b8-4822-a7a5-f6da7bae11ba|465981047|F801D728D8CFA95F173FC837627D1B671B07BE0D8164CFBF566FA5A146896F11',
  'b411daa0-8491-4f38-806a-290681cc2452|813291315|5339F75AD67A33037867458A767C4489F0B9D0CF3859C5E9C71667B2C27275D6',
  'd85a12cd-5723-42ff-b3bc-1df016ee76c4|408060978|F4555514118243F70FB35C1386A51A6A29BFC591CEDA0796C30F98032BA6FBC1',
  'e89fb6d5-63ed-4656-a8b8-7859c9324e58|389321150|F0D143A8CD54ED628B8304A3EBE4DA1872454F6830112B925AB0E055D74707C3',
  'fa4608b3-42d7-4ee0-9c94-9a0f2454eec0|932351683|65549B4B4B2B7B5D3114C685C29612448F70828CC20635356F921FAACF44E190',
  'fe912f30-1a12-4136-a5e6-d486a15ee669|926577538|DCC9B432D15737B6D3BE4C515C1DE7EB752FB85C7CCD2C5C7B6E85D6BF143776'
)
# Explicit single files (diagnostic dumps / stale logs) for this box.
$ExplicitFiles = @(
  'C:\Users\Public\list_C.txt|1',
  'C:\Users\Public\rc_C.txt|1',
  'C:\Users\Public\rc_find3.ps1|1'
)
$RecycleSid = 'S-1-5-21-924586991-3457137404-949529162-1001'
# ------------------------------------------------------------------------

$LogDir = 'C:\ProgramData\AutoRigCleanup'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Mode = 'DRYRUN'
if ($Apply) { $Mode = 'APPLY' }
$LogFile = Join-Path $LogDir ("{0}-cleanup-{1}-{2}.log" -f $Box, $Now.ToString('yyyyMMdd-HHmmss'), $Mode)

function Log([string]$m) {
  $line = "{0} {1}" -f (Get-Date).ToString('s'), $m
  Write-Output $line
  Add-Content -LiteralPath $LogFile -Value $line -Encoding ascii
}

function FreeGB { [math]::Round((Get-PSDrive C).Free / 1GB, 2) }

function Get-ConverterState {
  try {
    $r = Invoke-WebRequest -UseBasicParsing -Uri $StatusUrl -TimeoutSec 15
    $j = $r.Content | ConvertFrom-Json
  } catch {
    return [pscustomobject]@{ Ok = $false; Busy = $true; Text = "status unreachable: $($_.Exception.Message)"; Active = @() }
  }
  $s = $j.tasks_summary
  $proc = 0; $pend = 0; $queue = 0
  if ($s) { $proc = [int]$s.processing; $pend = [int]$s.pending; $queue = [int]$s.queue_size }
  $active = @()
  foreach ($t in @($j.processing_tasks) + @($j.pending_tasks)) {
    if ($null -eq $t) { continue }
    if ($t -is [string]) { $active += $t } else {
      foreach ($k in 'task_id', 'guid', 'id', 'model_name') { if ($t.PSObject.Properties[$k]) { $active += [string]$t.$k } }
    }
  }
  $busy = ($proc -gt 0) -or ($pend -gt 0) -or ($queue -gt 0) -or (@($active).Count -gt 0)
  return [pscustomobject]@{ Ok = $true; Busy = $busy; Text = "processing=$proc pending=$pend queue=$queue drift=$($j.deploy_drift)"; Active = $active }
}

function Wait-Idle([string]$why) {
  $deadline = (Get-Date).AddMinutes($WaitIdleMinutes)
  while ($true) {
    $st = Get-ConverterState
    if (-not $st.Busy) { Log "converter idle ($($st.Text)) before $why"; return $st }
    if (-not $Apply) { Log "WARNING converter busy ($($st.Text)); a real run would wait/refuse here. Continuing dry run."; return $st }
    if ((Get-Date) -gt $deadline) { Log "REFUSE: converter still busy after $WaitIdleMinutes min ($($st.Text)) before $why"; return $null }
    Log "converter busy ($($st.Text)); waiting 30 s before $why"
    Start-Sleep -Seconds 30
  }
}

# "In use" = some process has the file open for writing (or without sharing).
# Read-only handles that allow delete (nginx open_file_cache) do not count:
# Windows removes the file once those handles close, exactly as the
# converter's own periodic_cleanup does.
function Test-InUse([string]$p) {
  try { $fs = [IO.File]::Open($p, 'Open', 'Read', 'Read, Delete'); $fs.Close(); return $false } catch { return $true }
}

function Test-UnderRoot([string]$p, [string]$root) {
  $full = [IO.Path]::GetFullPath($p).TrimEnd('\')
  $r = [IO.Path]::GetFullPath($root).TrimEnd('\')
  return $full.StartsWith($r + '\', [StringComparison]::OrdinalIgnoreCase)
}

$Totals = [ordered]@{}
function Remove-Candidates([string]$cat, [string]$root, $files) {
  $n = 0; $bytes = 0; $skipped = 0
  foreach ($f in @($files)) {
    if ($null -eq $f) { continue }
    $p = $f.FullName
    if (-not (Test-UnderRoot $p $root)) { Log "  SKIP outside root: $p"; $skipped++; continue }
    if ($f.Attributes -band [IO.FileAttributes]::ReparsePoint) { Log "  SKIP reparse point: $p"; $skipped++; continue }
    if (Test-InUse $p) { Log "  SKIP in use: $p"; $skipped++; continue }
    $len = [int64]$f.Length
    if ($Apply) {
      try { Remove-Item -LiteralPath $p -Force; Log ("  DELETED {0,10:N1} MB {1:yyyy-MM-dd HH:mm} {2}" -f ($len / 1MB), $f.LastWriteTime, $p) }
      catch { Log "  FAILED $p : $($_.Exception.Message)"; $skipped++; continue }
    } else {
      Log ("  would delete {0,10:N1} MB {1:yyyy-MM-dd HH:mm} {2}" -f ($len / 1MB), $f.LastWriteTime, $p)
    }
    $n++; $bytes += $len
  }
  $Totals[$cat] = [math]::Round($bytes / 1GB, 2)
  Log ("[{0}] files={1} GB={2:N2} skipped={3}" -f $cat, $n, ($bytes / 1GB), $skipped)
}

function Remove-EmptyDirs([string]$root) {
  if (-not $Apply -or -not (Test-Path -LiteralPath $root)) { return }
  Get-ChildItem -LiteralPath $root -Recurse -Directory -Force -ErrorAction SilentlyContinue |
    Sort-Object { $_.FullName.Length } -Descending |
    Where-Object { -not ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -and -not (Get-ChildItem -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue) } |
    ForEach-Object { try { Remove-Item -LiteralPath $_.FullName -Force } catch {} }
}

# ================================================================== start
if ($env:COMPUTERNAME -ne $ExpectHost) { throw "REFUSE: this script is for $ExpectHost, this machine is $env:COMPUTERNAME" }
$FreeBefore = FreeGB
Log "=== $Box cleanup mode=$Mode host=$env:COMPUTERNAME free_before=${FreeBefore}GB log=$LogFile"
$st0 = Wait-Idle 'start'
if ($null -eq $st0) { exit 2 }
if ($Apply -and -not $st0.Ok) { Log 'REFUSE: converter status endpoint unreachable'; exit 2 }

# 1. Diagnostic dumps and stale logs (explicit paths only).
if ($null -eq (Wait-Idle 'explicit-files')) { exit 2 }
$ex = @()
foreach ($row in $ExplicitFiles) {
  $parts = $row.Split('|'); $p = $parts[0]; $minAgeDays = [double]$parts[1]
  if (-not (Test-Path -LiteralPath $p)) { Log "  absent: $p"; continue }
  $it = Get-Item -LiteralPath $p -Force
  if (($Now - $it.LastWriteTime).TotalDays -lt $minAgeDays) { Log "  SKIP modified within $minAgeDays d: $p"; continue }
  $ex += $it
}
Remove-Candidates 'explicit-dumps-and-stale-logs' 'C:\' $ex

# 1b. Read-only inventory scripts/output left by the 2026-09-23 audit (KB-sized).
$inv = 'C:\ProgramData\cc-inventory'
$c = Get-ChildItem -LiteralPath $inv -File -Force -ErrorAction SilentlyContinue
Remove-Candidates 'audit-inventory-scratch' $inv $c

# 2. Blender autosaves left in %TEMP% by converter Blender runs (not touched for 6 h).
if ($null -eq (Wait-Idle 'blender-autosave')) { exit 2 }
$c = Get-ChildItem -LiteralPath $Temp -File -Force -Filter '*_autosave.blend' -ErrorAction SilentlyContinue |
  Where-Object { ($Now - $_.LastWriteTime).TotalHours -gt 6 }
Remove-Candidates 'temp-blender-autosave' $Temp $c

# 3. OpenPose glog INFO logs in %TEMP% (one per run; closed after the run; older than 24 h).
if ($null -eq (Wait-Idle 'openpose-logs')) { exit 2 }
$c = Get-ChildItem -LiteralPath $Temp -File -Force -Filter 'OpenPose.*.log.*' -ErrorAction SilentlyContinue |
  Where-Object { ($Now - $_.LastWriteTime).TotalHours -gt 24 }
Remove-Candidates 'temp-openpose-logs' $Temp $c

# 4. Windows Error Reporting user crash dumps older than 14 days.
$cd = Join-Path $UserRoot 'AppData\Local\CrashDumps'
$c = Get-ChildItem -LiteralPath $cd -File -Force -Filter '*.dmp' -ErrorAction SilentlyContinue |
  Where-Object { ($Now - $_.LastWriteTime).TotalDays -gt 14 }
Remove-Candidates 'crashdumps-older-14d' $cd $c

# 5. pip download/wheel cache (pure cache; skipped while any pip process runs).
$pipRoot = Join-Path $UserRoot 'AppData\Local\pip\cache'
$pipBusy = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -match '(\s|\\|/)pip(3|\.exe)?(\s|$)|-m\s+pip' })
if ($pipBusy.Count -gt 0) {
  Log "[pip-cache] SKIP: pip process running (pid $($pipBusy[0].ProcessId))"; $Totals['pip-cache'] = 0
} else {
  $c = Get-ChildItem -LiteralPath $pipRoot -Recurse -File -Force -ErrorAction SilentlyContinue
  Remove-Candidates 'pip-cache' $pipRoot $c
  Remove-EmptyDirs $pipRoot
}

# 6. Converter <guid>.zip bundles already stored byte-identical on autorig.online.
if ($MirroredZips.Count -gt 0) {
  $st = Wait-Idle 'mirrored-zips'
  if ($null -eq $st) { exit 2 }
  $c = @()
  foreach ($row in $MirroredZips) {
    $parts = $row.Split('|'); $g = $parts[0]; $size = [int64]$parts[1]; $sha = $parts[2]
    if ($st.Active -contains $g) { Log "  SKIP active task $g"; continue }
    $zip = Join-Path $GlbRoot "$g.zip"
    if (-not (Test-Path -LiteralPath $zip)) { Log "  absent (already evicted by converter): $zip"; continue }
    $it = Get-Item -LiteralPath $zip -Force
    if ($it.Length -ne $size) { Log "  SKIP size differs from VPS copy ($($it.Length) != $size): $zip"; continue }
    $h = (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash
    if ($h -ne $sha) { Log "  SKIP sha256 differs from VPS copy: $zip"; continue }
    if (Test-InUse $zip) { Log "  SKIP in use: $zip"; continue }
    $c += $it
    $meta = Get-Item -LiteralPath "$zip.meta.json" -Force -ErrorAction SilentlyContinue
    if ($meta) { $c += $meta }
  }
  Remove-Candidates 'converter-zips-mirrored-on-vps' $GlbRoot $c
} else { Log '[converter-zips-mirrored-on-vps] none for this box' }

# 7. Optional: the user's Recycle Bin on C: (owner decision).
if ($IncludeRecycleBin -and $RecycleSid) {
  $rb = "C:\`$Recycle.Bin\$RecycleSid"
  $c = Get-ChildItem -LiteralPath $rb -Recurse -File -Force -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne 'desktop.ini' }
  Remove-Candidates 'recycle-bin' $rb $c
  Remove-EmptyDirs $rb
} else { Log '[recycle-bin] not included (use -IncludeRecycleBin)' }

$FreeAfter = FreeGB
$sum = 0; foreach ($k in $Totals.Keys) { $sum += $Totals[$k] }
Log "=== summary mode=$Mode"
foreach ($k in $Totals.Keys) { Log ("    {0,-34} {1,8:N2} GB" -f $k, $Totals[$k]) }
Log ("    {0,-34} {1,8:N2} GB" -f 'TOTAL', $sum)
Log "free_before=${FreeBefore}GB free_after=${FreeAfter}GB"
$stEnd = Get-ConverterState
Log "converter after: ok=$($stEnd.Ok) $($stEnd.Text)"
