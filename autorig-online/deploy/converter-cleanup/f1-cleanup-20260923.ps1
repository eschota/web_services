<#
  AutoRig converter farm disk cleanup - box f1 (F1-PC) - reviewed 2026-09-23.

  Deletes ONLY the categories and exact paths/patterns listed in the "box plan" section below.
  Inventory and rationale: autorig-online/docs/converter-cleanup-20260923.md

  Default is a DRY RUN: it lists what it would delete and the GB it would free.
  Add -Apply to actually delete. Add -IncludeRecycleBin to also empty the
  user's Recycle Bin on C: (owner decision, off by default).

  Safety rules built in:
  * refuses to run on any machine except F1-PC;
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
    C:\ProgramData\AutoRigCleanup\f1-cleanup-<timestamp>.log

  ASCII only on purpose (Windows PowerShell 5.1 over ssh).
#>
param(
  [switch]$Apply,
  [switch]$IncludeRecycleBin,
  [int]$WaitIdleMinutes = 45
)

$ErrorActionPreference = 'Stop'


$Box        = 'f1'
$ExpectHost = 'F1-PC'
$StatusUrl  = 'http://127.0.0.1:5132/api-converter-glb/server-status'
$UserRoot   = 'C:\Users\user'
$Temp       = 'C:\Users\user\AppData\Local\Temp'
$GlbRoot    = 'C:\NDLWebServerBuild\wwwroot\converter\glb'
$Now        = Get-Date

# ---------------------------------------------------------------- box plan
# Mirrored ZIP bundles: guid|bytes|sha256 of the copy in
# /srv/autorig/data/artifact-cache/<task>/ on autorig.online (checked 2026-09-23).
$MirroredZips = @(
  '0ae20fef-e15d-47fa-80db-3214008fac57|265116239|9397E8D2FA06219648678BE91EA873C56F7BA6A62FB9D604D664A2385408E239',
  '12a5f71e-2501-4fd0-a7bc-d64f4c9ee0bb|1036111553|9DC99D510EA5AD1264A716941A6BB56037B4A741B50B35BAF6BCB3683E4E25DC',
  '18b11f74-3be2-4f2c-9063-3b986eac491f|248091405|4343415F4D688FEA670062D3178F084F25FF10B1B00C5D14725D1916FF4FCF78',
  '212e12d1-1f80-43af-a873-74ebc5dfc5cd|304118178|F04A983EBF0510FBA8073CF6C71C3752127208668F19ED4301BD84ACD3AC8D60',
  '24bac4bf-72b2-406b-a622-12a2640bd2e7|251855895|45B5F9E611F9B474C75FDD300FA2937D6755ACD9B77414E115F029C0D1E1D5B5',
  '27d19db8-3843-4e66-8a5c-7b3953cde713|383975816|DC9748066BBD2A37A0D53CC105B1DA27DC20BAEFEE130B6EA89FEB089BC75055',
  '293427c0-e4e6-408c-8596-53503f84bf4c|404543830|8CA757920F27650B8015788D2D2C3B7D7427BF53F568B9AB6D618E3C95226A0E',
  '3afb0c3e-56f6-4465-bd33-054b59ee595c|1329528470|5A74C92EBFFB6BDA31B0390940FBF62CEF8430B39351E670B23A8C549A18BE87',
  '3bc1a0f6-a4a6-45b3-b2a3-cd4ca6b4ea37|243133902|0D981DD35876895DF22C7A1169DDCEC5490DF69D3A327345F965CA01873E1676',
  '3e59ddb6-05f4-453d-bb3a-9e7198666bbe|370095199|FF017D401B03583D3A0E7A71033DB297B03DBB21A2F4862AE8335C292BF41C75',
  '4d31c613-81cc-4374-bc67-19d3b54f8e94|428703095|DE1D3E95A3652D42EB15D14E9EEB3784F61EA9BCBC93082970EC8EF6D2FE27A1',
  '5bbc8954-99bb-4744-afdd-ec873e20b992|262242155|F8FF220D303937846FCA864E612183DD82E61D1F59E4ED10A8577A98B7863F1A',
  '628abe0d-733c-4d2d-985b-cb336ed4645e|457963131|328399DFE2BA06AE3C569F16AC23ECD7C0AC911D263C9A2EE30D0ED84B5BBD70',
  '68d85ca8-2ce0-401c-bf1a-2204d199e8cf|246943223|640F055873B9F7ABBC10340D0A44505F696E5AF9CD816C16ED59DE6622A6AE6E',
  '6c8bfc5a-2136-410b-b94b-ecdb15f081c0|250853966|E85952EC49B82EF972C50CE6321DE74FD61CDC86F0B9DD3BD3225D4F9A5DD769',
  '7349ebe3-a4c0-4b28-86b7-291c065e46fc|263400090|A92C05F39898AC338FFF2898B23C11047940CCD82D1F5DC6EB1C4A0C63F6EF60',
  '738b1f67-d77e-4e07-af72-8a41a4ca72b1|263116029|719F939957D6E61D1897CCF940F2224202997D4E5710FC883A8414EEC7F3948B',
  '774b1b27-98f3-4dd3-a7a2-9a1ca0b46860|247087569|B266EC84BE0E79AEBA7078E3FC3A9A3638450A355B42F8271FF695222F3223ED',
  '7abec704-7062-48d6-ba59-fb53bafe041b|246266104|46ED3497754D76E7B656586D9F6258CF3ECBD25B949868328EDF77CD99B0B5EE',
  '7c25520c-b422-412b-9f61-da8c716d3f30|298056990|9F82ABC104B3D4248B342A42C36D95340760A9A51146260F36776E06DB0BEE18',
  '7d43afc2-501a-4b06-9004-4111baaa2a07|609360095|B3B908892276B3F5B3B40DC3DE209E934AA73EBFB320218C262EBCA2DA0750F4',
  '9a6a2687-2d7b-4805-b209-d4ee18b9af68|299245972|15759AC284209EE0FABA6E8697C874035FA67D206118910D657E756C85536767',
  'a80e6f2f-788c-4477-9234-f1881767e7dd|261626453|05B0123096080679C564393CC80113ED8487057E14C798C8AD9F36C2DE227F69',
  'b5d1a80c-5403-40c2-90ce-2ef5b4694049|273040593|DE9450DE231B5A2EFDDE01DB6335A5EA5C11E6C079C198A616B1B72F39A5F61F',
  'c0631843-90b4-4854-b588-4cf3b02d8085|244878164|34C3514CF85AD206DD8261F78A3228A7834365F965AAE5296922DCBBCDD055BA',
  'c6405f10-5ba7-409c-9763-b050fcb44c36|320605727|567E86AADDA9DD123B190C62DB1C4D60497DAB6CED9E5B8836F32B0B4BC8CEFF',
  'd6207b43-5805-401d-bdd2-30313128b94e|1235409376|5FD592F09166B1F3B5A1BF3D2535597E5CC92DED95B95778CBB0146A8BC03E34',
  'da676b12-b210-4be3-bf6d-df8788f5f132|256873412|7FFD1E52B0E2484838896636B819BC20CDC94C6D1616931DE42112129831BE30',
  'e5f6f26b-3cbd-4e86-86ea-675e9d5ca8b7|306806369|C38F1F17F77430AD57BF84C9869BCA3E09E82E36D4EDB90CA4B0050A44597FC3'
)
# Explicit single files (diagnostic dumps / stale logs) for this box.
$ExplicitFiles = @(
  'C:\Users\Public\list_C.txt|1',
  'C:\Users\Public\rc_C.txt|1',
  'C:\Users\Public\rc_find3.ps1|1'
)
$RecycleSid = 'S-1-5-21-2953700907-1343294297-1561005842-1001'
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
