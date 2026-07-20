param(
    [string]$ValidationDirectory = "$env:TEMP\autorig-blender-validation-20260720"
)

$ErrorActionPreference = 'Stop'
$validator = Join-Path $ValidationDirectory 'validate_blender_artifacts.py'
$artifacts = @(
    'human_all_animations.fbx',
    'human_all_animations.glb',
    'human_all_animations.blend',
    'human_prepared.glb',
    'human_rigged.blend',
    'human_custom_chicken_dance.fbx',
    'animal_rabbit_pack.fbx',
    'animal_rabbit_animations.glb',
    'animal_rabbit_rigged.blend'
) | ForEach-Object { Join-Path $ValidationDirectory $_ }

$versions = [ordered]@{
    '4.3' = 'C:\Program Files\Blender Foundation\Blender 4.3\blender.exe'
    '5.1' = 'C:\Program Files\Blender Foundation\Blender 5.1\blender.exe'
}

$failedVersions = @()
foreach ($entry in $versions.GetEnumerator()) {
    $output = Join-Path $ValidationDirectory ("blender-{0}-report.json" -f $entry.Key)
    $arguments = @('-b', '--python', $validator, '--', '--output', $output) + $artifacts
    $quotedArguments = $arguments | ForEach-Object { '"' + ($_ -replace '"', '\"') + '"' }
    $process = Start-Process -FilePath $entry.Value -ArgumentList $quotedArguments -WindowStyle Hidden -PassThru
    try {
        $process.PriorityClass = [Diagnostics.ProcessPriorityClass]::BelowNormal
    }
    catch {
        Write-Warning "Could not lower Blender process priority: $_"
    }
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) {
        $failedVersions += $entry.Key
        Write-Warning "Blender $($entry.Key) reported invalid artifacts (exit code $($process.ExitCode))"
    }
    Get-Content -Raw $output | ConvertFrom-Json | Select-Object blender_version, @{Name='artifacts'; Expression={$_.artifacts.Count}}
}

if ($failedVersions.Count -gt 0) {
    throw "Artifact validation failed in Blender: $($failedVersions -join ', ')"
}
