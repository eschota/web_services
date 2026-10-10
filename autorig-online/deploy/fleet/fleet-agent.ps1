# AutoRig fleet agent: reports this box's disks, GPU, AutoRig scheduled tasks,
# listening ports and quarantine sizes to https://autorig.online/api/fleet/report
# so GET https://autorig.online/api/fleet shows every box in one call
# (owner rule 2026-10-10: fleet status is one ordinary API request).
#
# Read-only on the box: it deletes nothing, starts and stops nothing.
# Key: <home>\agent.key, else the LoRA sync key of the same box.
# Install (as an administrator):  fleet-agent.ps1 -Install -Box f1
#   registers the scheduled task "AutoRig Fleet Agent" (SYSTEM, every 2 minutes).
# worker-4090 (no elevation): fleet-agent.ps1 -Install -Box worker-4090 -UserTask -HomeDir <dir>
param([switch]$Install, [switch]$UserTask, [string]$Box = '', [string]$HomeDir = '')
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$AgentVersion = 'fleet-agent/2026-10-10c'
$Api = 'https://autorig.online/api/fleet'
$Home_ = if ($HomeDir) { $HomeDir } else { 'C:\ProgramData\AutoRig\fleet-agent' }
$LogFile = Join-Path $Home_ 'agent.log'
$StateFile = Join-Path $Home_ 'state.json'
$TaskName = 'AutoRig Fleet Agent'

function Log($m) {
    $line = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ' ' + $m
    try {
        if ((Test-Path $LogFile) -and ((Get-Item $LogFile).Length -gt 1MB)) { Move-Item -Force $LogFile ($LogFile + '.1') }
        Add-Content -Path $LogFile -Value $line -Encoding utf8
    } catch {}
    Write-Output $line
}

New-Item -ItemType Directory -Force $Home_ | Out-Null

if ($Install) {
    $self = Join-Path $Home_ 'fleet-agent.ps1'
    if ($MyInvocation.MyCommand.Path -and ($MyInvocation.MyCommand.Path -ne $self)) {
        Copy-Item -Force $MyInvocation.MyCommand.Path $self
    }
    if ($Box) { Set-Content -Path (Join-Path $Home_ 'box.txt') -Value $Box -Encoding ascii }
    $key = Join-Path $Home_ 'agent.key'
    if ($UserTask) {
        if (Test-Path $key) { icacls $key /inheritance:r /grant:r ($env:USERNAME + ':F') | Out-Null }
        $vbs = Join-Path $Home_ 'fleet-agent-hidden.vbs'
        $cmd = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File ""' + $self + '"" -HomeDir ""' + $Home_ + '""'
        Set-Content -Path $vbs -Encoding ascii -Value ('CreateObject("WScript.Shell").Run "' + $cmd + '", 0, True')
        schtasks /create /tn $TaskName /tr ('wscript.exe "' + $vbs + '"') /sc minute /mo 2 /f | Out-Null
    } else {
        if (Test-Path $key) { icacls $key /inheritance:r /grant:r 'SYSTEM:F' '*S-1-5-32-544:F' | Out-Null }
        $tr = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $self + '"'
        schtasks /create /tn $TaskName /tr $tr /sc minute /mo 2 /ru SYSTEM /rl HIGHEST /f | Out-Null
    }
    Log ('installed scheduled task ' + $TaskName)
    schtasks /run /tn $TaskName | Out-Null
    exit 0
}

try { $mutex = New-Object System.Threading.Mutex($false, 'Global\AutoRigFleetAgent') }
catch { $mutex = New-Object System.Threading.Mutex($false, 'Local\AutoRigFleetAgent') }
$owned = $false
try { $owned = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $owned = $true }
if (-not $owned) { Write-Output 'another run is active'; exit 0 }

function FirstLine($paths) {
    foreach ($p in $paths) {
        if ($p -and (Test-Path -LiteralPath $p)) {
            $v = (Get-Content -LiteralPath $p -TotalCount 1).Trim()
            if ($v) { return $v }
        }
    }
    return ''
}

try {
    $loraHomes = @('C:\ProgramData\AutoRig\lora-sync')
    if ($env:LOCALAPPDATA) { $loraHomes += (Join-Path $env:LOCALAPPDATA 'AutoRig\lora-sync') }
    if (-not $Box) {
        $Box = FirstLine (@((Join-Path $Home_ 'box.txt')) + ($loraHomes | ForEach-Object { Join-Path $_ 'box.txt' }))
    }
    if (-not $Box) { Log 'no box name (box.txt)'; exit 1 }
    $Key = FirstLine (@((Join-Path $Home_ 'agent.key')) + ($loraHomes | ForEach-Object { Join-Path $_ 'sync.key' }))
    if (-not $Key) { Log 'no agent.key'; exit 1 }

    # Self-update: the next run uses the agent the site serves.
    try {
        $self = Join-Path $Home_ 'fleet-agent.ps1'
        $fresh = (Invoke-WebRequest -UseBasicParsing -Uri ($Api + '/agent.ps1') -TimeoutSec 20).Content
        if ($fresh -is [byte[]]) { $fresh = [Text.Encoding]::UTF8.GetString($fresh) }
        if ($fresh -and $fresh.Contains('# AutoRig fleet agent') -and (Test-Path $self)) {
            $current = [IO.File]::ReadAllText($self)
            if ($current -ne $fresh) {
                $tmp = $self + '.new'
                [IO.File]::WriteAllText($tmp, $fresh, (New-Object Text.UTF8Encoding($false)))
                Move-Item -Force $tmp $self
                Log 'agent updated for the next run'
            }
        }
    } catch { Log ('agent update check failed: ' + $_.Exception.Message) }

    $state = @{}
    if (Test-Path $StateFile) {
        try { (Get-Content $StateFile -Raw | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $state[$_.Name] = $_.Value } } catch {}
    }

    $report = [ordered]@{ agent_version = $AgentVersion; host = $env:COMPUTERNAME }
    try {
        $os = Get-CimInstance Win32_OperatingSystem
        $report.boot_utc = $os.LastBootUpTime.ToUniversalTime().ToString('s') + 'Z'
        $report.ram_total_gb = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
        $report.ram_free_gb = [math]::Round($os.FreePhysicalMemory / 1MB, 1)
    } catch {}

    $drives = @()
    foreach ($d in (Get-CimInstance Win32_LogicalDisk -Filter 'DriveType=3')) {
        if (-not $d.Size) { continue }
        $drives += [ordered]@{ drive = $d.DeviceID; label = [string]$d.VolumeName
            size_gb = [math]::Round($d.Size / 1GB, 1); free_gb = [math]::Round($d.FreeSpace / 1GB, 1)
            used_percent = [math]::Round(100 * (1 - $d.FreeSpace / $d.Size), 1) }
    }
    $report.drives = $drives

    # Physical disk temperature and health (read-only). The NVMe/SATA temperature comes from
    # IOCTL_STORAGE_QUERY_PROPERTY (StorageDeviceTemperatureProperty), which needs no admin rights.
    # Added after worker-4090 crashed from an overheating SSD on 2026-10-10.
    try {
        if (-not ('AutoRigDiskTemp' -as [type])) {
            Add-Type -TypeDefinition @"
using System; using System.Runtime.InteropServices; using Microsoft.Win32.SafeHandles;
public static class AutoRigDiskTemp {
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern SafeFileHandle CreateFile(string n, uint a, uint s, IntPtr sa, uint cd, uint fl, IntPtr t);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool DeviceIoControl(SafeFileHandle h, uint code, byte[] inb, int inl, byte[] outb, int outl, out int ret, IntPtr ov);
  public static int[] Query(int n) {
    var h = CreateFile("\\\\.\\PhysicalDrive" + n, 0, 3, IntPtr.Zero, 3, 0, IntPtr.Zero);
    if (h.IsInvalid) return null;
    using (h) {
      byte[] q = new byte[12]; BitConverter.GetBytes(52).CopyTo(q, 0);
      byte[] o = new byte[512]; int r;
      if (!DeviceIoControl(h, 0x2D1400, q, q.Length, o, o.Length, out r, IntPtr.Zero)) return null;
      int cnt = BitConverter.ToUInt16(o, 12);
      if (cnt < 1) return null;
      return new int[] { BitConverter.ToInt16(o, 26), BitConverter.ToInt16(o, 10), BitConverter.ToInt16(o, 8) };
    }
  }
}
"@
        }
        $letters = @{}
        foreach ($pt in (Get-Partition -ErrorAction SilentlyContinue | Where-Object { $_.DriveLetter })) {
            $k = [string]$pt.DiskNumber
            if (-not $letters.ContainsKey($k)) { $letters[$k] = @() }
            $letters[$k] += ([string]$pt.DriveLetter + ':')
        }
        $dt = @()
        foreach ($pd in (Get-PhysicalDisk -ErrorAction SilentlyContinue)) {
            $num = [int]$pd.DeviceId
            $t = $null; try { $t = [AutoRigDiskTemp]::Query($num) } catch {}
            $dt += [ordered]@{ number = $num; name = [string]$pd.FriendlyName; media = [string]$pd.MediaType
                health = [string]$pd.HealthStatus; operational = [string]$pd.OperationalStatus
                temp_c = $(if ($t) { $t[0] } else { $null }); warn_c = $(if ($t) { $t[1] } else { $null })
                critical_c = $(if ($t) { $t[2] } else { $null })
                drives = @($letters[[string]$num]) }
        }
        $report.disk_health = $dt
    } catch { Log ('disk health failed: ' + $_.Exception.Message) }

    $smi = $null
    foreach ($c in @("$env:SystemRoot\System32\nvidia-smi.exe", 'C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe')) {
        if (Test-Path -LiteralPath $c) { $smi = $c; break }
    }
    if ($smi) {
        $ErrorActionPreference = 'Continue'
        $out = & $smi '--query-gpu=name,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu,driver_version' '--format=csv,noheader,nounits' 2>&1
        $rc = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'
        $first = (@($out) | Select-Object -First 1) -as [string]
        if ($rc -eq 0 -and $first -and $first.Contains(',')) {
            $p = $first.Split(',') | ForEach-Object { $_.Trim() }
            $report.gpu = [ordered]@{ name = $p[0]; vram_total_mb = [double]$p[1]; vram_used_mb = [double]$p[2]
                vram_free_mb = [double]$p[3]; util_percent = $(try { [double]$p[4] } catch { $null })
                temp_c = $(try { [double]$p[5] } catch { $null }); driver = $p[6]; error = '' }
        } else {
            $report.gpu = [ordered]@{ name = ''; error = ('nvidia_smi_exit_' + $rc) }
        }
    } else {
        $report.gpu = [ordered]@{ name = ''; error = 'nvidia_smi_missing' }
    }

    $tasks = @()
    try {
        foreach ($t in (Get-ScheduledTask -ErrorAction SilentlyContinue | Where-Object {
                    $_.TaskName -match 'AutoRig|Renderfin|renderfin|ComfyUI|Comfy|LoRA|LoraTrain|SECS|Tunnel|GLB Converter' })) {
            $i = $null
            try { $i = Get-ScheduledTaskInfo -TaskName $t.TaskName -TaskPath $t.TaskPath -ErrorAction SilentlyContinue } catch {}
            $tasks += [ordered]@{ name = $t.TaskName; state = [string]$t.State
                last_run = $(if ($i -and $i.LastRunTime) { $i.LastRunTime.ToUniversalTime().ToString('s') + 'Z' } else { '' })
                last_result = $(if ($i) { [int64]$i.LastTaskResult } else { $null }) }
        }
    } catch {}
    $report.tasks = $tasks

    $interesting = @(5131, 5132, 5198, 5199, 5267, 5279, 5480, 5533, 5541, 7000, 8092, 8093, 8188, 8288, 8289, 8488, 8588, 8988, 18777)
    try {
        $ports = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty LocalPort -Unique
        $report.listeners = @($ports | Where-Object { $interesting -contains $_ } | Sort-Object)
    } catch { $report.listeners = @() }

    $procs = [ordered]@{ blender = 0; unity = 0; llama = 0; python = 0; trainer = 0 }
    try {
        foreach ($p in (Get-CimInstance Win32_Process -Filter "Name='blender.exe' OR Name='Unity.exe' OR Name='llama-server.exe' OR Name='python.exe' OR Name='pythonw.exe'")) {
            switch -Regex ($p.Name) {
                '^blender' { $procs.blender++ }
                '^Unity' { $procs.unity++ }
                '^llama' { $procs.llama++ }
                '^python' {
                    $procs.python++
                    $cl = [string]$p.CommandLine
                    if ($cl -match 'train_network|sdxl_train|flux_train|train_lora|ai-toolkit|musubi|run\.py .*train|lora_train|kohya') { $procs.trainer++ }
                }
            }
        }
    } catch {}
    $report.processes = $procs

    # Quarantine folders at drive roots (_retired_*): sized at most every 6 hours.
    $q = @()
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $lastQ = 0
    if ($state.ContainsKey('quarantine_at')) { $lastQ = [int64]$state['quarantine_at'] }
    if (($now - $lastQ) -gt 21600 -or -not $state.ContainsKey('quarantine')) {
        foreach ($d in $drives) {
            foreach ($dir in (Get-ChildItem -LiteralPath ($d.drive + '\') -Directory -Force -ErrorAction SilentlyContinue | Where-Object { $_.Name -like '_retired_*' })) {
                $sum = 0
                Get-ChildItem -LiteralPath $dir.FullName -Recurse -File -Force -ErrorAction SilentlyContinue | ForEach-Object { $sum += $_.Length }
                $q += [ordered]@{ path = $dir.FullName; gb = [math]::Round($sum / 1GB, 2); measured_at = (Get-Date).ToUniversalTime().ToString('s') + 'Z' }
            }
        }
        $state['quarantine'] = $q
        $state['quarantine_at'] = $now
    } else {
        $q = @($state['quarantine'])
    }
    $report.quarantine = @($q | Where-Object { $_ })

    try { $state | ConvertTo-Json -Depth 5 -Compress | Set-Content -Path $StateFile -Encoding utf8 } catch {}

    $body = $report | ConvertTo-Json -Depth 6 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($body)
    $headers = @{ 'Authorization' = ('Bearer ' + $Key); 'X-AutoRig-Box' = $Box }
    try {
        Invoke-RestMethod -Uri ($Api + '/report') -Method Post -Headers $headers -Body $bytes `
            -ContentType 'application/json; charset=utf-8' -TimeoutSec 30 | Out-Null
    } catch { Log ('report failed: ' + $_.Exception.Message); exit 1 }
} catch {
    Log ('run failed: ' + $_.Exception.Message)
    exit 1
} finally {
    $mutex.ReleaseMutex()
}
