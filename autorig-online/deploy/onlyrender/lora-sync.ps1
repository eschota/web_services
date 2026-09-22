# AutoRig LoRA sync agent (render boxes: f5, f15, Raptor, f12, worker-4090).
#
# Pull model: every run asks https://autorig.online/api/ai/loras/sync/manifest
# which LoRAs this box should hold, fetches the missing ones (a LAN peer first
# when the manifest names one, then the VPS mirror), verifies SHA-256, moves
# them into ComfyUI's loras folder and reports the folder's whole inventory
# back. Every step is idempotent, so a box that was off catches up on its next
# run. ComfyUI is never touched: it lists new files in /object_info on its own.
#
# Files are never deleted. A LoRA removed at /lora, or a file the owner marked
# for cleanup, is moved to <ComfyUI root>\autorig_lora_trash\.
#
# Install (as an administrator):  lora-sync.ps1 -Install
#   needs C:\ProgramData\AutoRig\lora-sync\sync.key and box.txt
#   registers the scheduled task "AutoRig LoRA Sync" (SYSTEM, every 5 minutes).
param([switch]$Install, [string]$Box = '', [string]$ComfyRoot = '')
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$AgentVersion = 'lora-sync/2026-09-23'
$Api = 'https://autorig.online/api/ai/loras/sync'
$Home_ = 'C:\ProgramData\AutoRig\lora-sync'
$LogFile = Join-Path $Home_ 'sync.log'
$HashFile = Join-Path $Home_ 'hashes.json'
$TaskName = 'AutoRig LoRA Sync'

function Log($m) {
    $line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m
    try {
        if ((Test-Path $LogFile) -and ((Get-Item $LogFile).Length -gt 2MB)) {
            Move-Item -Force $LogFile ($LogFile + '.1')
        }
        Add-Content -Path $LogFile -Value $line -Encoding utf8
    } catch {}
    Write-Output $line
}

New-Item -ItemType Directory -Force $Home_ | Out-Null

# Task Scheduler's default priority 7 gives the task low I/O and memory
# priority: on Raptor hashing a 2 GB LoRA crawled for minutes while the same
# hash took 3 s interactively. Priority 5 is an ordinary process.
function SetNormalPriority {
    try {
        $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        if ($t.Settings.Priority -ne 5) {
            $t.Settings.Priority = 5
            $t | Set-ScheduledTask | Out-Null
            Log 'task priority set to 5 (takes effect next run)'
        }
    } catch { Log ('could not set task priority: ' + $_.Exception.Message) }
}

if ($Install) {
    $self = Join-Path $Home_ 'lora-sync.ps1'
    if ($MyInvocation.MyCommand.Path -and ($MyInvocation.MyCommand.Path -ne $self)) {
        Copy-Item -Force $MyInvocation.MyCommand.Path $self
    }
    if ($Box) { Set-Content -Path (Join-Path $Home_ 'box.txt') -Value $Box -Encoding ascii }
    if ($ComfyRoot) { Set-Content -Path (Join-Path $Home_ 'comfy_root.txt') -Value $ComfyRoot -Encoding ascii }
    # The key authenticates this box to the VPS: SYSTEM and administrators only.
    $key = Join-Path $Home_ 'sync.key'
    if (Test-Path $key) { icacls $key /inheritance:r /grant:r 'SYSTEM:F' '*S-1-5-32-544:F' | Out-Null }
    $tr = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $self + '"'
    schtasks /create /tn $TaskName /tr $tr /sc minute /mo 5 /ru SYSTEM /rl HIGHEST /f | Out-Null
    SetNormalPriority
    Log ('installed scheduled task ' + $TaskName)
    schtasks /run /tn $TaskName | Out-Null
    exit 0
}

# One run at a time: a scheduled run and an SSH nudge can overlap.
$mutex = New-Object System.Threading.Mutex($false, 'Global\AutoRigLoraSync')
# A run that was killed leaves the mutex abandoned; taking it over is fine.
$owned = $false
try { $owned = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $owned = $true }
if (-not $owned) { Write-Output 'another sync is running'; exit 0 }

try {
    if (-not $Box) {
        $boxFile = Join-Path $Home_ 'box.txt'
        if (Test-Path $boxFile) { $Box = (Get-Content $boxFile -TotalCount 1).Trim() }
    }
    if (-not $Box) { Log 'no box name (box.txt)'; exit 1 }
    SetNormalPriority
    $keyFile = Join-Path $Home_ 'sync.key'
    if (-not (Test-Path $keyFile)) { Log 'no sync.key'; exit 1 }
    $Key = (Get-Content $keyFile -TotalCount 1).Trim()
    $Headers = @{ 'Authorization' = ('Bearer ' + $Key); 'X-AutoRig-Box' = $Box }

    # Self-update: the next run uses the agent the site currently serves.
    try {
        $self = Join-Path $Home_ 'lora-sync.ps1'
        $fresh = (Invoke-WebRequest -UseBasicParsing -Uri ($Api + '/agent.ps1') -TimeoutSec 30).Content
        if ($fresh -is [byte[]]) { $fresh = [Text.Encoding]::UTF8.GetString($fresh) }
        if ($fresh -and $fresh.Contains('# AutoRig LoRA sync agent') -and (Test-Path $self)) {
            $current = [IO.File]::ReadAllText($self)
            if ($current -ne $fresh) {
                [IO.File]::WriteAllText($self, $fresh, (New-Object Text.UTF8Encoding($false)))
                Log 'agent updated for the next run'
            }
        }
    } catch { Log ('agent update check failed: ' + $_.Exception.Message) }

    if (-not $ComfyRoot) {
        $rootFile = Join-Path $Home_ 'comfy_root.txt'
        if (Test-Path $rootFile) { $ComfyRoot = (Get-Content $rootFile -TotalCount 1).Trim() }
    }
    if (-not $ComfyRoot) {
        # String concatenation, not Join-Path: PowerShell 5.1's Join-Path
        # throws on a drive letter the box does not have (f12 has no D:).
        foreach ($c in 'D:\ComfyUI_windows_portable', 'C:\AI\ComfyUI_windows_portable', 'C:\ComfyUI_windows_portable') {
            if (Test-Path -LiteralPath ($c + '\ComfyUI\main.py') -ErrorAction SilentlyContinue) { $ComfyRoot = $c; break }
        }
    }
    if (-not $ComfyRoot) { Log 'ComfyUI root not found'; exit 1 }
    $Loras = Join-Path $ComfyRoot 'ComfyUI\models\loras'
    $Tmp = Join-Path $ComfyRoot 'autorig_lora_tmp'
    $Trash = Join-Path $ComfyRoot 'autorig_lora_trash'
    New-Item -ItemType Directory -Force $Loras, $Tmp | Out-Null

    # Other folders ComfyUI reads LoRAs from (extra_model_paths.yaml), listed
    # for the inventory only; downloads always go to the main loras folder.
    $ExtraDirs = @()
    $yaml = Join-Path $ComfyRoot 'ComfyUI\extra_model_paths.yaml'
    if (Test-Path $yaml) {
        $basePath = ''; $inLoras = $false
        foreach ($raw in Get-Content $yaml) {
            $line = $raw.TrimEnd()
            if ($line -match '^\s*#' -or -not $line.Trim()) { continue }
            if ($line -match '^\S') { $basePath = ''; $inLoras = $false; continue }
            if ($line -match '^\s+base_path:\s*(.+)$') { $basePath = $Matches[1].Trim().Trim('"').Trim("'"); $inLoras = $false; continue }
            if ($line -match '^\s+loras:\s*\|?\s*$') { $inLoras = $true; continue }
            if ($line -match '^\s+loras:\s*(\S.*)$') { $inLoras = $false; $ExtraDirs += (Join-Path $basePath $Matches[1].Trim()); continue }
            if ($inLoras -and $line -match '^\s{6,}(\S.*)$') { $ExtraDirs += (Join-Path $basePath $Matches[1].Trim()); continue }
            if ($line -match '^\s{2,4}\S') { $inLoras = $false }
        }
    }
    $ExtraDirs = @($ExtraDirs | Where-Object { $_ -and (Test-Path $_) } | Select-Object -Unique)

    # SHA-256 cache keyed by path, size and write time: hashing a 7 GB file on
    # every run would make the agent the busiest thing on the disk.
    $cache = @{}
    if (Test-Path $HashFile) {
        try { (Get-Content $HashFile -Raw | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $cache[$_.Name] = $_.Value } } catch {}
    }
    function HashOf($file) {
        $k = $file.FullName + '|' + $file.Length + '|' + $file.LastWriteTimeUtc.Ticks
        if ($cache.ContainsKey($k)) { return $cache[$k] }
        if ($file.Length -gt 1GB) { Log ('hashing ' + $file.FullName + ' (' + [math]::Round($file.Length / 1GB, 1) + ' GB)') }
        $h = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLower()
        $cache[$k] = $h
        return $h
    }
    function Inventory() {
        $out = @()
        $dirs = @($Loras) + $ExtraDirs
        foreach ($d in $dirs) {
            Get-ChildItem -LiteralPath $d -Recurse -File -ErrorAction SilentlyContinue |
              Where-Object { $_.Extension -in '.safetensors', '.pt', '.ckpt', '.bin', '.pth' } |
              ForEach-Object {
                $rel = $_.FullName.Substring($d.Length).TrimStart('\')
                # Folders from extra_model_paths.yaml are listed for the
                # inventory only; their big files (a slow storage pool on
                # Raptor) are not worth reading end to end every new file.
                $sha = ''
                if ($d -eq $Loras -or $_.Length -lt 1GB) { $sha = (HashOf $_) }
                $out += [pscustomobject]@{ name = $rel; dir = $d; size = $_.Length;
                    mtime = [int64](($_.LastWriteTimeUtc - [datetime]'1970-01-01').TotalSeconds);
                    sha256 = $sha; full = $_.FullName }
              }
        }
        return $out
    }
    function Report($items, $inv) {
        $drive = (Get-Item $Loras).PSDrive.Name
        $free = (Get-PSDrive $drive).Free
        $files = @($inv | ForEach-Object { @{ name = $_.name; dir = $_.dir; size = $_.size; mtime = $_.mtime; sha256 = $_.sha256 } })
        $body = @{ loras_dirs = @(@($Loras) + $ExtraDirs); free_bytes = $free; files = $files;
                   items = $items; agent_version = $AgentVersion; comfy_root = $ComfyRoot } | ConvertTo-Json -Depth 6 -Compress
        $bytes = [Text.Encoding]::UTF8.GetBytes($body)
        try {
            Invoke-RestMethod -Uri ($Api + '/report') -Method Post -Headers $Headers -Body $bytes `
                -ContentType 'application/json; charset=utf-8' -TimeoutSec 60 | Out-Null
        } catch { Log ('report failed: ' + $_.Exception.Message) }
    }
    function MoveToTrash($full, $why) {
        New-Item -ItemType Directory -Force $Trash | Out-Null
        $dest = Join-Path $Trash ((Get-Date -Format 'yyyyMMdd-HHmmss') + '_' + (Split-Path $full -Leaf))
        Move-Item -LiteralPath $full -Destination $dest
        Log ('moved to trash (' + $why + '): ' + $full)
    }
    function Fetch($url, $dest, $useAuth) {
        # curl writes progress and errors to stderr; under 'Stop' PowerShell
        # 5.1 would turn that into a terminating error.
        $ErrorActionPreference = 'Continue'
        if (Test-Path $dest) { Remove-Item -Force $dest }
        $args_ = @('-sS', '-f', '-L', '--connect-timeout', '8', '--retry', '2', '--speed-time', '60', '--speed-limit', '2048', '-o', $dest)
        if ($useAuth) { $args_ += @('-H', ('Authorization: Bearer ' + $Key), '-H', ('X-AutoRig-Box: ' + $Box)) }
        $curl = Join-Path $env:SystemRoot 'System32\curl.exe'
        if (Test-Path $curl) {
            & $curl @args_ $url 2>&1 | Out-Null
            return ($LASTEXITCODE -eq 0 -and (Test-Path $dest))
        }
        try {
            if ($useAuth) { Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $dest -Headers $Headers -TimeoutSec 3600 }
            else { Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $dest -TimeoutSec 3600 }
            return (Test-Path $dest)
        } catch { return $false }
    }

    function SaveHashes() {
        try { $cache | ConvertTo-Json -Depth 3 -Compress | Set-Content -Path $HashFile -Encoding utf8 } catch {}
    }
    Log ('manifest from ' + $Api)
    $manifest = Invoke-RestMethod -Uri ($Api + '/manifest') -Headers $Headers -TimeoutSec 60
    Log ('manifest: ' + @($manifest.items_array).Count + ' items; hashing ' + (@($Loras) + $ExtraDirs -join ', '))
    $inv = @(Inventory)
    SaveHashes
    Log ('inventory: ' + $inv.Count + ' files')
    $protected = @($manifest.protected_array)
    $items = @{}

    # Removals and owner-requested cleanups first: they may free the space a
    # download needs.
    foreach ($r in @($manifest.remove_array)) {
        $hit = $inv | Where-Object { $_.dir -eq $Loras -and $_.name -eq $r.file -and $_.sha256 -eq $r.sha256 }
        foreach ($h in @($hit)) { MoveToTrash $h.full 'removed at /lora' }
    }
    foreach ($c in @($manifest.cleanup_array)) {
        if ($protected -contains $c.file) { Log ('refusing to clean protected ' + $c.file); continue }
        $hit = $inv | Where-Object { $_.name -eq $c.file -and (-not $c.sha256 -or $_.sha256 -eq $c.sha256) }
        foreach ($h in @($hit)) { MoveToTrash $h.full 'cleanup at /lora' }
    }
    $inv = @(Inventory)

    $todo = @()
    foreach ($it in @($manifest.items_array)) {
        $target = Join-Path $Loras $it.file
        $have = $inv | Where-Object { $_.dir -eq $Loras -and $_.name -eq $it.file } | Select-Object -First 1
        if ($have) {
            if ($have.sha256 -eq $it.sha256) { $items[$it.id] = @{ state = 'ready' } }
            else { $items[$it.id] = @{ state = 'hash_mismatch'; error = ('a different ' + $it.file + ' is already here') } }
            continue
        }
        # The same bytes under another name (a Civitai vs Hugging Face file
        # name): link instead of downloading again.
        $twin = $inv | Where-Object { $_.sha256 -eq $it.sha256 -and $_.full.Substring(0, 2) -eq $Loras.Substring(0, 2) } | Select-Object -First 1
        if ($twin) {
            try {
                New-Item -ItemType HardLink -Path $target -Target $twin.full | Out-Null
                $items[$it.id] = @{ state = 'ready' }
                Log ('linked ' + $it.file + ' to existing ' + $twin.full)
                continue
            } catch { Log ('hardlink failed: ' + $_.Exception.Message) }
        }
        $todo += $it
        $items[$it.id] = @{ state = 'downloading'; bytes = 0 }
    }
    if ($todo.Count -gt 0) { Report $items $inv }

    foreach ($it in $todo) {
        $target = Join-Path $Loras $it.file
        $part = Join-Path $Tmp ($it.sha256 + '.part')
        $drive = (Get-Item $Loras).PSDrive.Name
        if ((Get-PSDrive $drive).Free -lt ([int64]$it.size_bytes + 2GB)) {
            $items[$it.id] = @{ state = 'no_space'; error = 'less than size + 2 GB free' }
            continue
        }
        $ok = $false; $from = ''
        foreach ($peer in @($it.peers)) {
            if (-not $peer) { continue }
            if ((Fetch $peer $part $false) -and ((Get-FileHash -Algorithm SHA256 -LiteralPath $part).Hash.ToLower() -eq $it.sha256)) {
                $ok = $true; $from = $peer; break
            }
        }
        if (-not $ok) {
            if ((Fetch $it.url $part $true)) {
                $got = (Get-FileHash -Algorithm SHA256 -LiteralPath $part).Hash.ToLower()
                if ($got -eq $it.sha256) { $ok = $true; $from = 'vps' }
                else { $items[$it.id] = @{ state = 'hash_mismatch'; error = ('download hashed to ' + $got.Substring(0, 12)) } }
            } else {
                $items[$it.id] = @{ state = 'failed'; error = 'download failed' }
            }
        }
        if ($ok) {
            Move-Item -Force -LiteralPath $part -Destination $target
            $items[$it.id] = @{ state = 'ready' }
            Log ('installed ' + $it.file + ' from ' + $from)
        } elseif (Test-Path $part) { Remove-Item -Force $part }
    }

    $inv = @(Inventory)
    SaveHashes
    Log 'reporting'
    Report $items $inv
    Log ($Box + ': ' + @($manifest.items_array).Count + ' wanted, ' + $todo.Count + ' fetched, ' + $inv.Count + ' files listed')
} catch {
    Log ('sync failed: ' + $_.Exception.Message)
    exit 1
} finally {
    $mutex.ReleaseMutex()
}
