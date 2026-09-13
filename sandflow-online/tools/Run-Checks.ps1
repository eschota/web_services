[CmdletBinding()]
param([switch]$Publish)
$ErrorActionPreference='Stop'
$serviceRoot=Split-Path $PSScriptRoot -Parent
$env:DOTNET_CLI_HOME=Join-Path $serviceRoot '.work\dotnet-home'
$env:NUGET_PACKAGES=Join-Path $serviceRoot '.work\nuget'
$env:DOTNET_GENERATE_ASPNET_CERTIFICATE='false'
$env:DOTNET_CLI_TELEMETRY_OPTOUT='1'
$env:TEMP=Join-Path $serviceRoot '.work\temp'
$env:TMP=$env:TEMP
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
Push-Location $serviceRoot
try {
    dotnet run --project tests/SandFlow.Server.Tests.csproj -c Release -- (Join-Path $serviceRoot '.work\test-data')
    if ($LASTEXITCODE -ne 0) { throw 'Server test failure' }
    dotnet run --project voice/tests/SandFlow.Voice.Tests/SandFlow.Voice.Tests.csproj -c Release
    if ($LASTEXITCODE -ne 0) { throw 'Voice test failure' }
    if ($Publish) {
        dotnet publish server/SandFlow.Server.csproj -c Release -r linux-x64 --self-contained true -o .work/publish -v quiet
        if ($LASTEXITCODE -ne 0) { throw 'Linux publish failure' }
    }
}
finally { Pop-Location }
