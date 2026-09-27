# AutoRig farm disk clean (safe set): caches and old temp only.
# Never models, never user files, never the Recycle Bin, never AutoRig\migration-backups (the owner's call).
param([int]$TempDays = 2, [switch]$DryRun)
$log = 'C:\ProgramData\AutoRig\watchdogs\disk-clean.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
function FreeGB { [Math]::Round((Get-PSDrive C).Free / 1GB, 1) }
$before = FreeGB
$cutoff = (Get-Date).AddDays(-$TempDays)
$targets = @()
$targets += Get-ChildItem "$env:LOCALAPPDATA\pip\cache" -Recurse -File -Force -EA 0
$targets += Get-ChildItem "$env:LOCALAPPDATA\Temp" -Recurse -File -Force -EA 0 | Where-Object { $_.LastWriteTime -lt $cutoff }
$targets += Get-ChildItem 'C:\Windows\Temp' -Recurse -File -Force -EA 0 | Where-Object { $_.LastWriteTime -lt $cutoff }
# ComfyUI temp/output older than the same age, wherever a ComfyUI lives under C:\AI or D:\
foreach ($root in @('C:\AI', 'D:\')) {
    if (-not (Test-Path $root)) { continue }
    $dirs = Get-ChildItem $root -Recurse -Directory -Depth 4 -Force -EA 0 | Where-Object { ($_.Name -eq 'output' -or $_.Name -eq 'temp') -and $_.Parent.Name -eq 'ComfyUI' }
    foreach ($d in $dirs) {
        $targets += Get-ChildItem $d.FullName -Recurse -File -Force -EA 0 | Where-Object { $_.LastWriteTime -lt $cutoff }
    }
}
$sum = ($targets | Measure-Object Length -Sum).Sum
$n = 0
if (-not $DryRun) {
    foreach ($f in $targets) { try { Remove-Item -LiteralPath $f.FullName -Force -EA Stop; $n++ } catch {} }
}
$line = '{0} {1} files={2} deleted={3} size={4:N2}GB free_before={5}GB free_after={6}GB' -f (Get-Date -Format s), $env:COMPUTERNAME, $targets.Count, $n, ($sum / 1GB), $before, (FreeGB)
Add-Content -Path $log -Value $line
$line
