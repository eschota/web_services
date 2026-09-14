[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$WorldId)
$ErrorActionPreference='Stop'
if($WorldId -notmatch '^[0-9a-f]{32}$'){throw 'Exact world ID required'}
$serviceRoot=Split-Path $PSScriptRoot -Parent
$api='https://autorig.online/sandflow/api/v1'
$qaKey=(Get-Content -Raw -LiteralPath (Join-Path $serviceRoot '.work/qa-session/server.env')).Trim().Split('=',2)[1]
$guest=Invoke-RestMethod -Method Post -Uri ($api+'/sessions/guest') -Headers @{'X-SF-QA-Token'=$qaKey} -ContentType 'application/json' -Body '{}'
Remove-Variable qaKey
$headers=@{Authorization=('Bearer '+$guest.token)}
$joined=$false
try {
    $null=Invoke-RestMethod -Method Post -Uri ($api+'/worlds/'+$WorldId+'/join') -Headers $headers -ContentType 'application/json' -Body '{}'
    $joined=$true
    $folder=Join-Path $serviceRoot ('.work/tuning-inspect/'+[Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    $file=Join-Path $folder 'snapshot.sfs'
    Invoke-WebRequest -Uri ($api+'/worlds/'+$WorldId+'/snapshots/latest') -Headers $headers -OutFile $file
    [Reflection.Assembly]::LoadFrom((Join-Path $serviceRoot '.work/publish/SandFlow.Protocol.dll')) | Out-Null
    $state=[SandFlow.Protocol.SnapshotCodec]::Decode([IO.File]::ReadAllBytes($file))
    $tuning=$null;$options=$null
    foreach($section in $state.Sections){
        if($section.Name -eq 'tuning-v1'){$tuning=[SandFlow.Protocol.TuningSettings]::Decode($section.Data)}
        if($section.Name -eq 'world-options-v1'){$options=[SandFlow.Protocol.WorldOptions]::Decode($section.Data)}
    }
    if($null -eq $tuning){throw 'Saved state has no tuning section'}
    $defaults=[SandFlow.Protocol.CanonicalTuningDefaults]::Create()
    [ordered]@{
        worldId=$state.WorldId;epoch=$state.Epoch;tick=$state.Tick;sequence=$state.Sequence
        metadata=$state.MetadataJson;tuningCount=$tuning.Count;TintFloor=$tuning['TintFloor']
        canonicalTintFloor=$defaults['TintFloor'];ErosionStrength=$tuning['ErosionStrength']
        worldOptions=$options
        containsLocalPixelDensity=$tuning.ContainsKey('PixelDensity');fileSha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $file).Hash
        localFile=$file
    } | ConvertTo-Json -Depth 4
}
finally {
    if($joined){$null=Invoke-RestMethod -Method Post -Uri ($api+'/worlds/'+$WorldId+'/leave') -Headers $headers}
    Remove-Variable guest,headers
}
