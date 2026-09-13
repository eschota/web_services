# SandFlow browser voice controller

This package is the browser media boundary for SandFlow's self-hosted LiveKit server. It is pinned to the official `livekit-client` **2.21.0** package and does not use LiveKit Cloud.

## Install, build, and test

Node modules are local and ignored; npm cache and compiler output stay under `.work`:

```powershell
Set-Location R:\autorig\sandflow-online\voice\client-web
$env:npm_config_cache = 'R:\autorig\sandflow-online\voice\client-web\.work\npm-cache'
npm ci --ignore-scripts --no-audit --no-fund
npm test
```

`package-lock.json` pins the full dependency graph. TypeScript uses `skipLibCheck` only because LiveKit 2.21.0's published declarations reference build-time transformer declarations that are not shipped to consumers; all SandFlow source remains under `strict`, `noUncheckedIndexedAccess`, and `exactOptionalPropertyTypes` checking.

## Public integration API

```ts
const tokenProvider = new HttpVoiceTokenProvider({
  apiBaseUrl: '/sandflow/api/v1',
  // Omit getBearerSession to use the same-origin HttpOnly sf_session cookie.
});

const controller = new VoiceMediaController({
  tokenProvider,
  roomFactory: new LiveKitBrowserRoomFactory(hiddenAudioContainer),
  callbacks: {
    onStateChanged: renderConnectionIcon,
    onMicrophoneChanged: renderMicIcon,
    onActiveSpeakersChanged: animateSpeakingModels,
    onAudioPlaybackBlocked: showPhysicalStartAudioControl,
    onError: reportBoundedVoiceError,
  },
});

await controller.switchMembership({ sandboxId });

// Explicit player opt-in. This only arms PTT; it does not capture audio.
controller.enableMicrophone();

physicalPttControl.addEventListener('pointerdown', () => {
  void controller.setPushToTalkPressed(true).catch(() => undefined); // onError already receives a bounded code
});
physicalPttControl.addEventListener('pointerup', () => {
  void controller.setPushToTalkPressed(false).catch(() => undefined);
});
physicalPttControl.addEventListener('pointercancel', () => {
  void controller.setPushToTalkPressed(false).catch(() => undefined);
});

await controller.setSelfMuted(true);
controller.setRemoteMuted(participantId, true);
controller.setRemoteVolume(participantId, 0.5); // 0..1
controller.setRemoteBlocked(participantId, true);

// Invoke directly from click/tap if the browser blocks autoplay.
await controller.startAudioFromUserGesture();

await controller.disconnect();
```

`sandboxId` is the backend's 32-character lowercase GUID `N` format. `switchMembership` always aborts the previous token request, resets PTT, requests immediate teardown of the old microphone/room, and requests a fresh server-authorized token. It accepts the server-derived `roomName` only when it belongs to the requested sandbox and matches `sf:{guid}:all` or `sf:{guid}:team:{0|1}`. It never accepts a client-provided team.

The HTTP provider accepts exactly the same-origin `/sandflow/api/v1` prefix. Scheme-relative paths, backslashes, encoded paths, queries, fragments, traversal and trailing-slash variants are rejected before `fetch`; an optional bearer session must match the backend's 64-hex session-token contract. LiveKit media is likewise pinned to exactly `wss://autorig.online/sandflow/voice`, so even a validly shaped token response cannot redirect microphone media to another WSS host.

`enableMicrophone()` records explicit consent but performs no media call. With the default PTT policy, the first `setPushToTalkPressed(true)` after consent is the first operation allowed to request microphone permission. The adapter deliberately separates `createLocalAudioTrack` from `publishTrack`: after capture resolves it rechecks the operation generation, and a capture revoked while the permission prompt was pending is stopped before `publishTrack` is called. Concurrent/repeated enable requests share one pending acquisition, and an existing microphone publication is never acquired or published again. The exact acquired `LocalAudioTrack` is passed to LiveKit rather than wrapping its raw `MediaStreamTrack` a second time. PTT release, full consent opt-out, membership transition, and room teardown unpublish with `stopOnUnpublish=true`. Room disconnect is initiated without awaiting a potentially unresolved browser permission prompt. Self mute, consent revocation, membership transition, disconnect, and stale async operations all converge on microphone disabled. The UI integration must also release PTT on window blur, document visibility loss, scene unload, and input-device loss.

### Audio profile

The adapter exports `SANDFLOW_AUDIO_CAPTURE_OPTIONS` and `SANDFLOW_AUDIO_PUBLISH_DEFAULTS`; both use `satisfies` against the pinned SDK's `AudioCaptureOptions` and `TrackPublishDefaults`, so an SDK upgrade that removes or changes a knob fails compilation.

- Capture requests exactly one channel with echo cancellation, noise suppression, and automatic gain control.
- The speech target is a maximum average bitrate of **32 kbit/s**, within the approved 24–32 kbit/s initial range.
- `forceStereo=false` keeps the publication mono.
- `dtx=true` requests Opus discontinuous transmission during silence.
- `red=true` requests LiveKit/WebRTC redundant audio where negotiation supports it.
- `preConnectBuffer=false` prevents pre-consent/pre-connect microphone buffering.

The SDK does not expose an application-level “force Opus” selector or a separate in-band FEC switch in `TrackPublishDefaults`. Opus/RED, DTX and recovery behavior are negotiated by browser and server SDP; RED may add bandwidth overhead, and unsupported options may not take effect. The 32 kbit/s value is an encoding target/ceiling, not measured wire bandwidth or a quality guarantee.

Stopping the browser track on every PTT release is an implementation privacy choice, not a user-stated requirement. It can make subsequent presses reacquire the device and can change Bluetooth profiles. PTT press-to-audio latency and device behavior are currently unmeasured; this configuration is not yet proof of an optimized fast-PTT path. If browser QA shows unacceptable latency, retain mandatory full opt-out unpublish/stop but evaluate keeping capture warm between PTT presses only after explicit consent as a documented privacy/performance trade-off.

Remote mute, volume, and block are local listening preferences. Blocking detaches the remote audio element and filters that identity from active-speaker callbacks. Persist these preferences in the authenticated account layer if they must survive devices; this package intentionally contains no storage.

## SDK behavior used

- `Room.connect` / `Room.disconnect` with automatic audio subscription.
- `createLocalAudioTrack`, a generation/intent gate, `LocalParticipant.publishTrack`, and `unpublishTrack(..., true)`; no camera or screen-share API is called.
- `TrackSubscribed`, `TrackUnsubscribed`, `ActiveSpeakersChanged`, `Reconnecting`, `Reconnected`, `Disconnected`, `MediaDevicesError`, and `AudioPlaybackStatusChanged` events.
- `RemoteAudioTrack.attach`, `detach`, and `setVolume`.
- `Room.startAudio`, which must be called from a user gesture when browser autoplay policy requires it.

Primary references: [official LiveKit JavaScript SDK](https://github.com/livekit/client-sdk-js), [microphone enable implementation](https://github.com/livekit/client-sdk-js/blob/main/src/room/participant/LocalParticipant.ts), and [LiveKit audio playback guidance](https://docs.livekit.io/home/client/tracks/subscribe/#audio-playback).

## Honest validation boundary

The tests use a deterministic mocked room SDK and cover lifecycle, PTT consent, self mute, in-flight and never-resolving permission races, pre-publication revocation, repeated-enable coalescing, existing-publication idempotence, microphone denial, team-token transition, cross-sandbox and cross-host token rejection, strict same-origin HTTP paths, fetch-not-called failures, reconnect intent, remote mute/volume/block, active-speaker filtering, and the pinned mono/32-kbit/DTX/RED option contract.

They do **not** prove real browser permission prompts, audio capture/playback, TURN fallback, Windows/WebGPU crossplay, real reconnection timing, or end-to-end sound quality. No browser or local HTTP server was launched. Public admission and voice flags remain disabled until authenticated production-like browser QA.

The planned Russian/English profanity detector and shared 60-second voice/chat mute remain analysis only. No ASR model, transcript capture, or moderation runtime is included here. The approved reactive design may allow the triggering phrase to be heard before server enforcement.
