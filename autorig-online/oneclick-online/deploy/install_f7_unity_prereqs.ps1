$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$codeRoot = [IO.Path]::GetFullPath('C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer')
$workRoot = [IO.Path]::GetFullPath((Join-Path $codeRoot 'work\oneclick-unity-prereqs'))
$editorRoot = [IO.Path]::GetFullPath('C:\Program Files\Unity\Hub\Editor\6000.0.44f1')
$unityExe = Join-Path $editorRoot 'Editor\Unity.exe'
$playbackRoot = Join-Path $editorRoot 'Editor\Data\PlaybackEngines'
$markerPath = Join-Path $codeRoot '.converter_maintenance'
$statusUri = 'http://127.0.0.1:5131/api-converter-glb/server-status'
$ownedMarker = $false
$markerContent = $null
$startUtc = [DateTime]::UtcNow
$cFreeBefore = (Get-PSDrive -Name C).Free

function Assert-Under([string]$Path, [string]$Parent, [string]$Label) {
    $resolved = [IO.Path]::GetFullPath($Path).TrimEnd('\')
    $root = [IO.Path]::GetFullPath($Parent).TrimEnd('\')
    if (-not $resolved.StartsWith($root + '\', [StringComparison]::OrdinalIgnoreCase) -and
        $resolved -ine $root) {
        throw "$Label escapes its approved root: $resolved"
    }
    return $resolved
}

function Get-Status {
    Invoke-RestMethod -Uri $statusUri -Method Get -TimeoutSec 15
}

function Assert-Idle([string]$Stage) {
    $status = Get-Status
    $pending = @($status.pending_tasks).Count
    $processing = @($status.processing_tasks).Count
    $queue = [int]$status.tasks_summary.queue_size
    $hunyuan = [int]$status.hunyuan.queue_size
    $processEmpty = [bool]$status.workload_control.process_control.empty
    if ($pending -ne 0 -or $processing -ne 0 -or $queue -ne 0 -or
        $hunyuan -ne 0 -or -not $processEmpty -or $status.hunyuan.active_task) {
        throw "F7 is not idle at $Stage (pending=$pending processing=$processing queue=$queue hunyuan=$hunyuan processEmpty=$processEmpty)."
    }
    if (@(Get-Process -Name 'Unity' -ErrorAction SilentlyContinue).Count -ne 0) {
        throw "Unity.exe is active at $Stage."
    }
    Write-Output "IDLE_OK stage=$Stage"
}

function New-OwnedMarker {
    if (Test-Path -LiteralPath $markerPath) {
        throw "Maintenance marker already exists: $markerPath"
    }
    $process = Get-Process -Id $PID -ErrorAction Stop
    $started = [DateTimeOffset]::new($process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds()
    $script:markerContent = (
        "protocol=oneclick_unity_prerequisites_v1`n" +
        "owner_pid=$PID`n" +
        "owner_started_at_unix_ms=$started`n" +
        "reason=install OneClick Unity 6000.0.44f1 Android and WebGL prerequisites`n" +
        "nonce=$([guid]::NewGuid().ToString('N'))`n"
    )
    $temporary = Join-Path $codeRoot ('.converter_maintenance.{0}.tmp' -f [guid]::NewGuid().ToString('N'))
    try {
        $stream = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $writer = [IO.StreamWriter]::new($stream, [Text.UTF8Encoding]::new($false))
        try {
            $writer.Write($script:markerContent)
            $writer.Flush()
            $stream.Flush($true)
        }
        finally { $writer.Dispose() }
        [IO.File]::Move($temporary, $markerPath)
        $script:ownedMarker = $true
        if ((Get-Content -LiteralPath $markerPath -Raw) -cne $script:markerContent) {
            throw 'Owned maintenance marker content verification failed.'
        }
    }
    finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
    Write-Output "MAINTENANCE_CLAIMED pid=$PID"
}

function Remove-OwnedMarker {
    if (-not $script:ownedMarker) { return }
    if (-not (Test-Path -LiteralPath $markerPath -PathType Leaf)) {
        throw 'Owned maintenance marker vanished unexpectedly.'
    }
    if ((Get-Content -LiteralPath $markerPath -Raw) -cne $script:markerContent) {
        throw 'Refusing to remove maintenance marker because its ownership content changed.'
    }
    Remove-Item -LiteralPath $markerPath -Force
    $script:ownedMarker = $false
    Write-Output 'MAINTENANCE_RELEASED'
}

function Download-Official([pscustomobject]$Item) {
    if (-not ([Uri]$Item.url).Scheme.Equals('https', [StringComparison]::OrdinalIgnoreCase)) {
        throw "Non-HTTPS URL rejected: $($Item.url)"
    }
    $destination = Assert-Under (Join-Path $workRoot ('downloads\' + $Item.file)) $workRoot 'Download path'
    $parent = Split-Path -Parent $destination
    [IO.Directory]::CreateDirectory($parent) | Out-Null
    $head = Invoke-WebRequest -Uri $Item.url -Method Head -UseBasicParsing -MaximumRedirection 5
    $vendorIdentityLength = $null
    if ($head.Headers['X-Identity-Content-Length']) {
        $vendorIdentityLength = [int64]($head.Headers['X-Identity-Content-Length'] | Select-Object -First 1)
    }
    elseif ($head.Headers['Content-Length']) {
        $vendorIdentityLength = [int64]($head.Headers['Content-Length'] | Select-Object -First 1)
    }
    $Item | Add-Member -NotePropertyName vendor_identity_bytes -NotePropertyValue $vendorIdentityLength -Force
    $Item | Add-Member -NotePropertyName vendor_etag -NotePropertyValue ([string]($head.Headers['ETag'] | Select-Object -First 1)) -Force
    $Item | Add-Member -NotePropertyName vendor_last_modified -NotePropertyValue ([string]($head.Headers['Last-Modified'] | Select-Object -First 1)) -Force
    if (Test-Path -LiteralPath $destination -PathType Leaf) {
        Write-Output "DOWNLOAD_REUSE_VERIFY id=$($Item.id) url=$($Item.url)"
    }
    else {
        Write-Output "DOWNLOAD_START id=$($Item.id) expected_bytes=$($Item.bytes) url=$($Item.url)"
        Invoke-WebRequest -Uri $Item.url -OutFile $destination -UseBasicParsing -MaximumRedirection 5
    }
    $file = Get-Item -LiteralPath $destination
    if ($file.Length -ne [int64]$Item.bytes) {
        $sizeDelta = [math]::Abs($file.Length - [int64]$Item.bytes)
        $hasDigest = $Item.md5 -or $Item.sha256 -or $Item.sha384
        $vendorLengthMatches = $vendorIdentityLength -and $file.Length -eq $vendorIdentityLength
        if ((-not $hasDigest -and -not $vendorLengthMatches) -or ($hasDigest -and $sizeDelta -gt 1023)) {
            throw "Downloaded size mismatch without a binding digest or exact vendor identity length for $($Item.id): $($file.Length) != $($Item.bytes)"
        }
        $gate = if ($hasDigest) { 'cryptographic' } else { 'vendor_identity_length' }
        Write-Output "DOWNLOAD_SIZE_DRIFT id=$($Item.id) actual=$($file.Length) manifest=$($Item.bytes) gate=$gate"
    }
    $sha256 = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Item.sha256 -and $sha256 -cne $Item.sha256) {
        throw "SHA-256 mismatch for $($Item.id)."
    }
    if ($Item.md5) {
        $md5 = (Get-FileHash -LiteralPath $destination -Algorithm MD5).Hash.ToLowerInvariant()
        if ($md5 -cne $Item.md5) { throw "Manifest MD5 mismatch for $($Item.id)." }
    }
    if ($Item.sha384) {
        $sha384 = (Get-FileHash -LiteralPath $destination -Algorithm SHA384).Hash.ToLowerInvariant()
        if ($sha384 -cne $Item.sha384) { throw "Manifest SHA-384 mismatch for $($Item.id)." }
    }
    if ($Item.type -eq 'exe') {
        $signature = Get-AuthenticodeSignature -LiteralPath $destination
        if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'Unity Technologies') {
            throw "Unity Authenticode validation failed for $($Item.id): status=$($signature.Status) subject=$($signature.SignerCertificate.Subject)"
        }
        $Item | Add-Member -NotePropertyName signer -NotePropertyValue $signature.SignerCertificate.Subject -Force
    }
    $Item | Add-Member -NotePropertyName local_path -NotePropertyValue $destination -Force
    $Item | Add-Member -NotePropertyName actual_bytes -NotePropertyValue $file.Length -Force
    $Item | Add-Member -NotePropertyName sha256_actual -NotePropertyValue $sha256 -Force
    Write-Output "DOWNLOAD_OK id=$($Item.id) bytes=$($file.Length) sha256=$sha256"
}

function Install-UnityModule([pscustomobject]$Item) {
    Write-Output "INSTALL_START id=$($Item.id)"
    $process = Start-Process -FilePath $Item.local_path -ArgumentList "/S /D=$editorRoot" -WindowStyle Hidden -Wait -PassThru
    if ($process.ExitCode -ne 0) { throw "Unity module installer $($Item.id) exited $($process.ExitCode)." }
    Write-Output "INSTALL_OK id=$($Item.id) exit=0"
}

function Expand-To([pscustomobject]$Item, [string]$Destination) {
    $destinationPath = Assert-Under $Destination $editorRoot "Extraction destination for $($Item.id)"
    [IO.Directory]::CreateDirectory($destinationPath) | Out-Null
    Write-Output "EXTRACT_START id=$($Item.id) destination=$destinationPath"
    Expand-Archive -LiteralPath $Item.local_path -DestinationPath $destinationPath -Force
    Write-Output "EXTRACT_OK id=$($Item.id)"
}

function Rename-Exact([string]$From, [string]$To, [string]$Label) {
    $source = Assert-Under $From $editorRoot "$Label source"
    $target = Assert-Under $To $editorRoot "$Label target"
    if (-not (Test-Path -LiteralPath $source)) { throw "$Label source is missing: $source" }
    if (Test-Path -LiteralPath $target) { throw "$Label target already exists: $target" }
    Move-Item -LiteralPath $source -Destination $target
}

$items = @(
    [pscustomobject]@{id='android';type='exe';file='UnitySetup-Android-Support-for-Editor-6000.0.44f1.exe';bytes=471401472;md5='567d64765c75b94312829fff105caaf8';sha256=$null;sha384=$null;url='https://download.unity3d.com/download_unity/101c91f3a8fb/TargetSupportInstaller/UnitySetup-Android-Support-for-Editor-6000.0.44f1.exe'},
    [pscustomobject]@{id='webgl';type='exe';file='UnitySetup-WebGL-Support-for-Editor-6000.0.44f1.exe';bytes=902988800;md5='3bd09f3bc24c5cd18fcf4f9f68ea4f65';sha256=$null;sha384=$null;url='https://download.unity3d.com/download_unity/101c91f3a8fb/TargetSupportInstaller/UnitySetup-WebGL-Support-for-Editor-6000.0.44f1.exe'},
    [pscustomobject]@{id='openjdk-17.0.9+9';type='zip';file='jdk17.0.9-9.zip';bytes=117700264;md5=$null;sha256='f12c2989c2f749b13282640a12d7d624097f6c2d45144d87331f21ad352ab63e';sha384=$null;url='https://download.unity3d.com/download_unity/open-jdk/open-jdk-win-x64/jdk17.0.9-9_f12c2989c2f749b13282640a12d7d624097f6c2d45144d87331f21ad352ab63e.zip'},
    [pscustomobject]@{id='sdk-tools-26.1.1';type='zip';file='sdk-tools-windows-4333796.zip';bytes=148000000;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/sdk-tools-windows-4333796.zip'},
    [pscustomobject]@{id='ndk-r27c';type='zip';file='android-ndk-r27c-windows.zip';bytes=781511249;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/android-ndk-r27c-windows.zip'},
    [pscustomobject]@{id='cmake-3.22.1';type='zip';file='cmake-3.22.1-windows.zip';bytes=16116742;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/cmake-3.22.1-windows.zip'},
    [pscustomobject]@{id='build-tools-34.0.0';type='zip';file='build-tools_r34-windows.zip';bytes=58253258;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/build-tools_r34-windows.zip'},
    [pscustomobject]@{id='platform-tools-34.0.5';type='zip';file='platform-tools_r34.0.5-windows.zip';bytes=5909544;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/platform-tools_r34.0.5-windows.zip'},
    [pscustomobject]@{id='platforms-33';type='zip';file='platform-33_r02.zip';bytes=67334130;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/platform-33_r02.zip'},
    [pscustomobject]@{id='platforms-34';type='zip';file='platform-34-ext7_r02.zip';bytes=63180079;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/platform-34-ext7_r02.zip'},
    [pscustomobject]@{id='platforms-35';type='zip';file='platform-35_r01.zip';bytes=64281654;md5=$null;sha256=$null;sha384=$null;url='https://dl.google.com/android/repository/platform-35_r01.zip'},
    [pscustomobject]@{id='cmdline-tools-6.0';type='zip';file='commandlinetools-win-8092744_latest.zip';bytes=119629490;md5=$null;sha256=$null;sha384='e86c58f83753d245bebba6312de8a3183833511fab0e7e0ae1fdf743f6b0082453f966ab12c913b6d9e488016cba4035';url='https://dl.google.com/android/repository/commandlinetools-win-8092744_latest.zip'}
)

try {
    if (-not [Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Provisioning shell is not elevated; refusing partial installation.'
    }
    if (-not (Test-Path -LiteralPath $unityExe -PathType Leaf)) { throw "Exact Unity editor is missing: $unityExe" }
    if ((Get-Item -LiteralPath $unityExe).VersionInfo.FileVersion -notlike '6000.0.44.*') {
        throw 'Exact Unity editor file version check failed.'
    }
    Assert-Under $workRoot $codeRoot 'Work root' | Out-Null
    Assert-Idle 'before-maintenance'
    New-OwnedMarker
    Start-Sleep -Seconds 3
    $ack = Get-Status
    if (-not $ack.maintenance) { throw 'Converter did not acknowledge maintenance.' }
    Assert-Idle 'after-maintenance'
    [IO.Directory]::CreateDirectory($workRoot) | Out-Null

    $androidBase = Join-Path $playbackRoot 'AndroidPlayer'
    $webglBase = Join-Path $playbackRoot 'WebGLSupport'
    $androidPresent = Test-Path -LiteralPath $androidBase -PathType Container
    $webglPresent = Test-Path -LiteralPath $webglBase -PathType Container
    if ($androidPresent -xor $webglPresent) {
        throw 'Only one base target module exists; refusing an ambiguous partial base-module state.'
    }
    $installBaseModules = -not $androidPresent
    if (-not $installBaseModules) {
        foreach ($anchor in @(
            (Join-Path $androidBase 'AndroidPlayerBuildProgram.exe'),
            (Join-Path $androidBase 'modules.asset'),
            (Join-Path $webglBase 'WebGLPlayerBuildProgram.exe'),
            (Join-Path $webglBase 'modules.asset')
        )) {
            if (-not (Test-Path -LiteralPath $anchor -PathType Leaf)) {
                throw "Installed base module is missing its anchor: $anchor"
            }
        }
        Write-Output 'BASE_MODULES_REUSE android=true webgl=true'
    }

    foreach ($item in $items) { Download-Official $item }
    if ($installBaseModules) {
        Install-UnityModule ($items | Where-Object id -eq 'android')
        Install-UnityModule ($items | Where-Object id -eq 'webgl')
    }

    $androidRoot = Join-Path $playbackRoot 'AndroidPlayer'
    if (-not (Test-Path -LiteralPath $androidRoot -PathType Container)) { throw 'Android support installer produced no AndroidPlayer directory.' }
    if (-not (Test-Path -LiteralPath (Join-Path $playbackRoot 'WebGLSupport') -PathType Container)) { throw 'WebGL support installer produced no WebGLSupport directory.' }

    $submoduleAnchors = @(
        (Join-Path $androidRoot 'OpenJDK\bin\java.exe'),
        (Join-Path $androidRoot 'NDK\source.properties'),
        (Join-Path $androidRoot 'SDK\cmake\3.22.1\bin\cmake.exe'),
        (Join-Path $androidRoot 'SDK\build-tools\34.0.0\aapt2.exe'),
        (Join-Path $androidRoot 'SDK\platform-tools\adb.exe'),
        (Join-Path $androidRoot 'SDK\platforms\android-33\android.jar'),
        (Join-Path $androidRoot 'SDK\platforms\android-34\android.jar'),
        (Join-Path $androidRoot 'SDK\platforms\android-35\android.jar'),
        (Join-Path $androidRoot 'SDK\cmdline-tools\6.0\bin\sdkmanager.bat')
    )
    $submodulesComplete = @($submoduleAnchors | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) }).Count -eq 0
    if ($submodulesComplete) {
        Write-Output 'ANDROID_SUBMODULES_REUSE complete=true'
    }
    else {
    $jdkItem = $items | Where-Object id -eq 'openjdk-17.0.9+9'
    $jdkStage = Assert-Under (Join-Path $workRoot 'extract\jdk') $workRoot 'JDK staging'
    if (Test-Path -LiteralPath $jdkStage) { Remove-Item -LiteralPath $jdkStage -Recurse -Force }
    [IO.Directory]::CreateDirectory($jdkStage) | Out-Null
    Expand-Archive -LiteralPath $jdkItem.local_path -DestinationPath $jdkStage -Force
    $jdkTarget = Assert-Under (Join-Path $androidRoot 'OpenJDK') $editorRoot 'OpenJDK target'
    [IO.Directory]::CreateDirectory($jdkTarget) | Out-Null
    Get-ChildItem -LiteralPath $jdkStage -Force |
        Copy-Item -Destination $jdkTarget -Recurse -Force

    Expand-To ($items | Where-Object id -eq 'sdk-tools-26.1.1') (Join-Path $androidRoot 'SDK')
    Expand-To ($items | Where-Object id -eq 'ndk-r27c') (Join-Path $androidRoot 'NDK')
    Rename-Exact (Join-Path $androidRoot 'NDK\android-ndk-r27c') (Join-Path $androidRoot 'NDK.__contents') 'NDK flatten stage'
    $ndkContainer = Join-Path $androidRoot 'NDK'
    $ndkContents = Join-Path $androidRoot 'NDK.__contents'
    Remove-Item -LiteralPath $ndkContainer -Force
    Move-Item -LiteralPath $ndkContents -Destination $ndkContainer

    Expand-To ($items | Where-Object id -eq 'cmake-3.22.1') (Join-Path $androidRoot 'SDK\cmake\cmake')
    Rename-Exact (Join-Path $androidRoot 'SDK\cmake\cmake') (Join-Path $androidRoot 'SDK\cmake\3.22.1') 'CMake version directory'
    Expand-To ($items | Where-Object id -eq 'build-tools-34.0.0') (Join-Path $androidRoot 'SDK\build-tools')
    Rename-Exact (Join-Path $androidRoot 'SDK\build-tools\android-14') (Join-Path $androidRoot 'SDK\build-tools\34.0.0') 'Build-tools version directory'
    Expand-To ($items | Where-Object id -eq 'platform-tools-34.0.5') (Join-Path $androidRoot 'SDK')
    Expand-To ($items | Where-Object id -eq 'platforms-33') (Join-Path $androidRoot 'SDK\platforms')
    Rename-Exact (Join-Path $androidRoot 'SDK\platforms\android-13') (Join-Path $androidRoot 'SDK\platforms\android-33') 'Android 33 platform directory'
    Expand-To ($items | Where-Object id -eq 'platforms-34') (Join-Path $androidRoot 'SDK\platforms')
    Expand-To ($items | Where-Object id -eq 'platforms-35') (Join-Path $androidRoot 'SDK\platforms')
    Expand-To ($items | Where-Object id -eq 'cmdline-tools-6.0') (Join-Path $androidRoot 'SDK\cmdline-tools')
    Rename-Exact (Join-Path $androidRoot 'SDK\cmdline-tools\cmdline-tools') (Join-Path $androidRoot 'SDK\cmdline-tools\6.0') 'Command-line tools version directory'
    }

    $required = @($submoduleAnchors) + @(
        (Join-Path $playbackRoot 'WebGLSupport\BuildTools\Emscripten\emscripten\emcc.py')
    )
    foreach ($path in $required) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required module file missing: $path" }
    }
    $savedErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $javaVersion = (& (Join-Path $androidRoot 'OpenJDK\bin\java.exe') -version 2>&1 | Out-String).Trim()
        $javaExitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $savedErrorAction }
    if ($javaExitCode -ne 0) { throw "OpenJDK version probe exited $javaExitCode." }
    $ndkVersion = (Get-Content -LiteralPath (Join-Path $androidRoot 'NDK\source.properties') | Where-Object { $_ -like 'Pkg.Revision*' }) -join '; '

    $probeRoot = Assert-Under (Join-Path $workRoot 'probe-project') $workRoot 'Probe root'
    [IO.Directory]::CreateDirectory((Join-Path $probeRoot 'Assets\Editor')) | Out-Null
    [IO.Directory]::CreateDirectory((Join-Path $probeRoot 'ProjectSettings')) | Out-Null
    Set-Content -LiteralPath (Join-Path $probeRoot 'ProjectSettings\ProjectVersion.txt') -Encoding UTF8 -Value "m_EditorVersion: 6000.0.44f1`nm_EditorVersionWithRevision: 6000.0.44f1 (101c91f3a8fb)"
    $probeOutput = Join-Path $workRoot 'unity-target-probe.json'
    $probeSource = @"
using System.IO;
using UnityEditor;
public static class OneClickPrereqProbe {
    public static void Run() {
        bool android = BuildPipeline.IsBuildTargetSupported(BuildTargetGroup.Android, BuildTarget.Android);
        bool webgl = BuildPipeline.IsBuildTargetSupported(BuildTargetGroup.WebGL, BuildTarget.WebGL);
        File.WriteAllText(@"$probeOutput", "{\"android\":" + android.ToString().ToLowerInvariant() + ",\"webgl\":" + webgl.ToString().ToLowerInvariant() + "}");
        if (!android || !webgl) EditorApplication.Exit(3);
    }
}
"@
    Set-Content -LiteralPath (Join-Path $probeRoot 'Assets\Editor\OneClickPrereqProbe.cs') -Encoding UTF8 -Value $probeSource
    $probeLog = Join-Path $workRoot 'unity-target-probe.log'
    Write-Output 'UNITY_PROBE_START'
    $probe = Start-Process -FilePath $unityExe -ArgumentList @('-batchmode','-nographics','-quit','-projectPath',"`"$probeRoot`"",'-executeMethod','OneClickPrereqProbe.Run','-logFile',"`"$probeLog`"") -WindowStyle Hidden -Wait -PassThru
    if ($probe.ExitCode -ne 0) { throw "Unity target probe exited $($probe.ExitCode). See $probeLog" }
    $probeResult = Get-Content -LiteralPath $probeOutput -Raw | ConvertFrom-Json
    if (-not $probeResult.android -or -not $probeResult.webgl) { throw 'Unity target probe did not recognize both targets.' }
    Write-Output 'UNITY_PROBE_OK android=true webgl=true'

    $receipt = [ordered]@{
        schema = 1
        editor = '6000.0.44f1'
        revision = '101c91f3a8fb'
        completed_at_utc = [DateTime]::UtcNow.ToString('o')
        elapsed_seconds = [math]::Round(([DateTime]::UtcNow - $startUtc).TotalSeconds, 1)
        c_free_before_bytes = $cFreeBefore
        c_free_after_bytes = (Get-PSDrive -Name C).Free
        java_version = $javaVersion
        ndk_version = $ndkVersion
        unity_probe = $probeResult
        downloads = @($items | Select-Object id,url,bytes,actual_bytes,vendor_identity_bytes,vendor_etag,vendor_last_modified,md5,sha256,sha384,sha256_actual,signer)
        required_files = @($required | ForEach-Object { [ordered]@{path=$_;sha256=(Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash.ToLowerInvariant()} })
    }
    $receiptPath = Join-Path $workRoot 'provision-receipt.json'
    $receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
    Write-Output "RECEIPT path=$receiptPath"

    $downloadsPath = Assert-Under (Join-Path $workRoot 'downloads') $workRoot 'Downloads cleanup path'
    $extractPath = Assert-Under (Join-Path $workRoot 'extract') $workRoot 'Extract cleanup path'
    if (Test-Path -LiteralPath $downloadsPath) { Remove-Item -LiteralPath $downloadsPath -Recurse -Force }
    if (Test-Path -LiteralPath $extractPath) { Remove-Item -LiteralPath $extractPath -Recurse -Force }
    Write-Output 'TEMP_DOWNLOADS_CLEANED'
}
finally {
    Remove-OwnedMarker
}
