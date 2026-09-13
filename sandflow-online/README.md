# SandFlow Online — isolated backend

Approved plan/live log: https://github.com/eschota/Sand-Keeper/tree/main/docs . Delivery: `eschota/web_services:main`. Physics runs **only on clients**.

## Current status (2026-09-13)

Foundation candidate, not a launched multiplayer game. Public admission and voice are OFF by default.

Implemented and exercised by direct .NET tests:

- hashed guest bearer sessions, public/private metadata, PBKDF2 passwords, demo admission limits;
- ordered bounded input queues, client host leases, epoch fencing, stale/replayed input rejection;
- SQLite WAL metadata, checksum-verified atomic versioned physical-field snapshots and 10-version history;
- dormant empty rooms, member kick/block, separate WSS control/state routing;
- delegated LiveKit token library with microphone-only grants and team-room isolation.

Not complete: Unity online adapter/gateway, full actor/settings snapshot coverage and tile repair/replay, full settings validation, world reset/rollback, Steam authentication/entitlement, all full-game maps, actual voice media integration, UFO/PvP, physical menus, capacity/browser/crossplay QA. The portable snapshot codec now bounds decompression, validates physical field shape/finiteness and binds world/epoch/tick/sequence; the Unity capture currently covers physical fields only. Public admission stays disabled until integration/QA. No 100-player capacity claim.

Moderation analysis: `voice/docs/MODERATION_ANALYSIS.md`. Owner selected reactive RU/EN recognition with a 60-second communication mute; first offending speech may already be audible. Runtime ASR/moderation is not implemented.

## Build and tests

Use .NET 10. Set `DOTNET_CLI_HOME`, `NUGET_PACKAGES`, `TEMP` and `TMP` under this service's `.work`; set `DOTNET_CLI_TELEMETRY_OPTOUT=1` and `DOTNET_GENERATE_ASPNET_CERTIFICATE=false`.

```powershell
dotnet run --project tests/SandFlow.Server.Tests.csproj -c Release -- R:\autorig\sandflow-online\.work\test-data
dotnet run --project voice/tests/SandFlow.Voice.Tests/SandFlow.Voice.Tests.csproj -c Release
dotnet publish server/SandFlow.Server.csproj -c Release -r linux-x64 --self-contained true -o .work/publish
```

Tests instantiate room/storage state directly. They launch no local web server and do not establish GPU/network gameplay quality.

## Runtime contract

- `SANDFLOW_DATA`: dedicated persistent directory, never an AutoRig data path.
- `SANDFLOW_ADMISSION_ENABLED`: false by default; explicit true only after client QA.
- `SANDFLOW_VOICE_*`: documented in voice/README.md, fail closed when absent.
- Bind to a dedicated loopback port, proposed 8270. `/health` reports the real gate and stage. Nginx must strip `/sandflow/` before proxying API/WS.
- `/ws/{worldId}/control` and `/ws/{worldId}/state`, protocol 1. Secure session cookie or Authorization bearer header; never put authentication tokens in URLs.
- Unavailable Steam/reset/voice operations return explicit errors, not simulated success.

Deploy SandFlow-only immutable release directories, not the repository root. Existing AutoRig route/process health must remain unchanged across any scoped nginx reload.
