# SandFlow voice module

This directory contains an isolated token-issuing library and a pinned, non-deployed LiveKit template. It does not modify or restart AutoRig services.

## Backend contract

The parent backend must authenticate the player, admit them to a sandbox, and assign their team before constructing `VoiceMembership`. Never copy room, team, admission, or speaking claims from a client request.

```csharp
var voice = new VoiceTokenService(VoiceTokenOptions.FromEnvironment(), TimeProvider.System);
var result = voice.IssueToken(new VoiceMembership(
    sandbox.Id,
    authenticatedPlayer.Id,
    sandbox.IsPvp ? VoiceMode.Pvp : VoiceMode.Coop,
    serverAssignedTeamId,
    CanSpeak: !authenticatedPlayer.IsVoiceBanned,
    IsAdmitted: admission.IsValid));
```

The result contains `ServerUrl`, JWT `Token`, exact `RoomName`, `ParticipantId`, and `ExpiresAt`. Tokens live for five minutes and grant room join/subscription plus microphone-only publication. Co-op rooms are `sf:{sandboxId}:all`; PvP teams are physically isolated as `sf:{sandboxId}:team:0` and `sf:{sandboxId}:team:1`.

Configuration is fail-closed. `SANDFLOW_VOICE_ENABLED=true` is required together with:

- `SANDFLOW_VOICE_SERVER_URL=wss://autorig.online/sandflow/voice`
- `SANDFLOW_VOICE_API_KEY`
- `SANDFLOW_VOICE_API_SECRET` (at least 32 characters)

## Client integration boundary

- Native Windows: pin and wrap the official `livekit/client-sdk-unity` package. Prefer Platform Audio for echo cancellation, noise suppression, and automatic gain control; fall back only after device QA.
- Unity WebGPU/WebGL: pin and wrap `livekit/client-sdk-unity-web`. Unity's built-in `Microphone` API is not supported on WebGL; browser capture must remain inside the SDK/browser `getUserMedia` path over HTTPS.
- Both adapters consume only `ServerUrl` and `Token`, start muted, and request microphone access only after an explicit player gesture. Default UX is push-to-talk; voice activation is opt-in.
- Local mute/volume/block state belongs to the game account. A server voice ban sets `CanSpeak=false` or denies admission. No recording/egress service is included.
- Reconnect requests a fresh backend token. Clients never reuse an expired token or derive a room/team locally.

This repository does not yet contain the Unity adapters, microphone UI, persistence, moderation endpoints, or end-to-end media QA. Those require integration with the canonical game and parent backend.

## Deployment template (not applied)

The image is pinned to LiveKit `v1.13.6` and its multi-platform manifest digest. Copy `livekit.example.yaml` to the ignored `livekit.yaml`, replace the key and secret, install TLS files in the ignored `certs/`, then validate the config and firewall before enabling the example unit.

Required dedicated endpoints:

- API/signaling `127.0.0.1:7880`, reverse-proxied as WSS under `/sandflow/voice`.
- ICE/TCP `7881/tcp`.
- ICE/UDP mux `7882/udp`.
- TURN/TLS `5349/tcp` on `turn.sandflow.autorig.online`.

Port 443 remains owned by the existing web proxy. No nginx, firewall, DNS, certificate, systemd, Docker, or production service change is performed by this module.

## Tests

Keep all build artifacts inside `voice/.work`:

```powershell
$env:DOTNET_CLI_HOME = 'R:\autorig\sandflow-online\voice\.work\dotnet-home'
$env:NUGET_PACKAGES = 'R:\autorig\sandflow-online\voice\.work\nuget'
dotnet run --project R:\autorig\sandflow-online\voice\tests\SandFlow.Voice.Tests\SandFlow.Voice.Tests.csproj
```

The console harness covers team-room isolation, admission denial, microphone-only grants, listener grants, deterministic expiry/signature verification, invalid configuration, and invalid membership.
