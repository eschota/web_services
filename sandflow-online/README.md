# SandFlow Online — isolated backend

Approved plan/live log: https://github.com/eschota/Sand-Keeper/tree/main/docs . Delivery: `eschota/web_services:main`. Physics runs **only on clients**.

## Current status (2026-09-13)

Foundation candidate, not a launched multiplayer game. Public admission and voice are OFF by default. The nginx root route `/sandflow/` now maps to the existing public single-player prototype release `sandflow-v0135-f957395` (already served at `/realflow/`). This compatibility route does not enable new multiplayer or change `map.autorig.online`.

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

`tools/SnapshotCompare` decodes two real native checkpoint files and rejects different worlds, epochs, ticks, command cursors or grids before comparing physical fields. Float limits are fixed at absolute 1e-4 plus relative 1e-6; integer flags compare exactly. A passing physical-only result is not complete actor/settings or visual acceptance. Self-comparison passes and the earlier different-tick native captures correctly fail comparability.

2026-09-13 continuation: the client fixed asynchronous GPU readback lifetime and passed a delayed-consumer 21-field restore check. The next private native pair saved successfully but disconnected early; same-tick agreement remains unverified. The service now logs only fixed WebSocket rejection codes or exception type (not raw messages, credentials or passwords) before sibling-task cancellation can obscure the actual cause. Unit suite: 43 server assertions and 10 voice tests pass. Public admission stays closed.

2026-09-14 continuation: snapshot v2 carries bounded non-cell module sections while decoding v1 remains supported. The Unity candidate now captures obstacle and draggable state and orders validated actor pose/carry/release/settle actions alongside brush input. Server suite passes 56 assertions. Native same-tick revalidation is pending; previous comparable captures FAILED physical agreement. SnapshotCompare now checks captured section bytes and metadata as well as fields; it still cannot certify missing schema coverage or rendering. Public admission stays closed.

The next candidate adds portable 32x32 digest/repair codecs and a bounded checkpoint ring, plus scheduled checkpoint flags in commit batches. State kind 2 routes a participant's digest only to authority; kind 3 routes an authority repair only to the addressed same-room participant. Peer identity, epoch, scheduled tick, chunk bounds, byte rate and authority rights are checked. `repairing` excludes a rewinding follower from host election. `peer_resync` is authority-only. Suite passes 85 assertions plus 10 voice-service checks; voice/client-web separately passes 13 mocked lifecycle/media-gate tests. None establishes live voice or 100-player gameplay.

An offline repair of the actual divergent native capture pair passed field tolerances and exact module state: digest 17,277 bytes + repair 1,598,802 bytes versus full snapshot 2,505,269 bytes. This one capture-pair size comparison is not a runtime bandwidth/FPS optimization claim. The Unity GPU fixture also passed historical repair followed by 30 steps/two recorded commands. The live periodic client integration still needs validation. In particular, seamless authority changes during rewind, full settings/actor coverage and high-player command-tail retention remain open acceptance work.

## Runtime contract

- `SANDFLOW_DATA`: dedicated persistent directory, never an AutoRig data path.
- `SANDFLOW_ADMISSION_ENABLED`: false by default; explicit true only after client QA.
- `SANDFLOW_VOICE_*`: documented in voice/README.md, fail closed when absent.
- Bind to a dedicated loopback port, proposed 8270. `/health` reports the real gate and stage. Nginx must strip `/sandflow/` before proxying API/WS.
- `/ws/{worldId}/control` and `/ws/{worldId}/state`, protocol 1. Secure session cookie or Authorization bearer header; never put authentication tokens in URLs.
- Unavailable Steam/reset/voice operations return explicit errors, not simulated success.

Deploy SandFlow-only immutable release directories, not the repository root. Existing AutoRig route/process health must remain unchanged across any scoped nginx reload.

`deploy/update-nginx-snippet.sh` updates only the existing SandFlow snippet after checking its prior hash, candidate hash and pinned WebGPU payload presence. It backs up the old snippet inside the service project, runs `nginx -t`, reloads nginx and restores the exact prior snippet if validation fails. The existing HTML uses relative `Build/` and `TemplateData/` paths, so all client resources are served under the new prefix. [nginx alias](https://nginx.org/en/docs/http/ngx_http_core_module.html#alias), [configuration reload](https://nginx.org/en/docs/control.html).
